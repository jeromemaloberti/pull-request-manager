import re, os, time, traceback
from watchdog import Watchdog
from github import Github
from jiralib import jira

# switch, which determines whether any real actions are executed
active = True

# set basic variables
bot_name = "xen-git"
tmp_branch = "%s-tmp" % bot_name
import settings # bot_email, bot_api_token, builds_path
org_name = "xen-org"
rep_names = { # repository names to corresponding component names
    'filesystem-summarise' : 'filesystem-summarise',
    'stunnel' : 'stunnel',
    'xen-api': 'api',
    'xen-api-libs': 'api-libs',
    }
build_dir = "build-%s.hg" % bot_name
log_file = "build-%s.log" % bot_name
log_path = "%s/%s" % (settings.builds_path, log_file)
build_path = "%s/%s" % (settings.builds_path, build_dir)
build_rep_prefix = "http://hg/carbon"
branch_whitelist = { # valid GitHub branch -> local branch
    'master' : 'trunk-ring3',
    'boston-lcm' : 'boston-lcm',
    'rrdd' : 'rrdd',
    'tampa' : 'tampa',
    'sanibel-lcm' : 'sanibel-lcm',
    }
short_sleep = 60 # seconds
long_sleep = 600 # seconds
resync_sleep = 300 # seconds
fetch_timeout = 180 # seconds
build_timeout = 1800 # seconds

# result caches
branch_sha_cache = {}

# prepare 'positive' and 'close' admin comment regular expression
ls = [l.strip() for l in open("positive.txt").readlines() if l.strip()]
positive = '|'.join(["(%s)" % l for l in ls])

# create an authenticating GitHub client
github = Github(bot_name, settings.bot_password)
org = github.get_organization(org_name)

def refresh_privileges():
    """Figure out who can approve a request (admin_usernames), and whose pull
    requests are considered (pr_usernames)."""
    log("Refreshing privileges..")
    global admin_usernames, pr_usernames
    teams = org.get_teams()
    admin_teams = [t for t in teams if t.permission in ["admin", "push"]]
    admins = sum([list(team.get_members()) for team in admin_teams], [])
    admin_usernames = [admin.login for admin in admins]
    pr_team_ids = [t for t in teams if t.name == "Authorised pull request authors"]
    pr_users = sum([list(team.get_members()) for team in pr_team_ids], [])
    pr_usernames = [pr_user.login for pr_user in pr_users]
    pr_usernames.extend(admin_usernames)

def get_next_pull_request():
    """Performs a fresh search, and obtains the next pull request to process,
    whether a re-build is required for this pull request, whether the pull
    request should be merged, and what (if any) ticket to close."""
    log("Searching for pull request to process..")
    backup_pr = None
    # for each repository
    for rep_name in rep_names:
        # get repository path
        repo = org.get_repo(rep_name)
        # fetch all open pull requests for this repository
        all_prs = repo.get_pulls("open")
        # select only pull requests by trusted users
        valid_prs = [pr for pr in all_prs
                     if pr.user.login in pr_usernames
                     and pr.base.ref in branch_whitelist]
        # Select pull requests which are not made by trusted users,
        # but which 1) have comments 2) made by admin users 3) which
        # contain the text '@xen-git check' in the comment body.
        for pr in set(all_prs) - set(valid_prs):
            # Strange bug in github api, comments are in issues.
            issue = repo.get_issue(pr.number)
            comments = issue.get_comments()
            if search_comments(comments, "check",admin_usernames):
                valid_prs.append(pr)
        # if a pull request contains a specific comment, chose it immediately
        # otherwise, choose a pull request with no comments from bot or whose
        # refs have changed
        for valid_pr in valid_prs:
            # Strange bug in github api, comments are in issues.
            issue = repo.get_issue(valid_pr.number)
            comments = issue.get_comments()
            succeeded, new_pr, changed = should_rebuild(valid_pr, comments)
            send_notification = new_pr
            validators = admin_usernames
            if valid_pr.base.ref == "tampa": validators = ['benchalmers']
            # check if an admin approved it, and its last attempt to build it
            # was successful or refs have changed
            if (succeeded or changed) and search_comments(comments, positive,validators):
                log("APPROVED: %s/%d" % (rep_name, valid_pr.number))
                ticket = search_title_for_key(valid_pr)
                log("TICKET: %s" % ticket)
                return valid_pr, True, True, ticket, send_notification # rebuild, merge, to close
            # otherwise, check if it should be processed anyway
            if changed: backup_pr = valid_pr
    return backup_pr, True, False, None, send_notification # rebuild, don't merge, don't close

def search_comments(comments, search_re,validators):
    """Checks whether any comment of a pull request starts with "@xen-git",
    and one of its parts (parts are delimited by '.' or '!') starts with the
    given regular expression (case ignored)."""
    for c in comments:
        if c.user.login not in validators: continue
        m = re.match("@%s " % bot_name, c.body, re.I)
        if not m: continue
        cmds = c.body[m.end():].replace('!', '.').split('.')
        cmds = [cmd.strip() for cmd in cmds if cmd.strip()]
        for cmd in cmds:
            if re.search(search_re, cmd, re.I | re.U):
                return cmd

def dependencies_satisfied(pr, rep_name):
    """Checks that all the pull requests that this pull request depends on have
    been merged. The format for specifying dependencies is:
       Dependencies: (<pr_number>@<rep_name_without_org_name>)*
    Multiple dependencies are separated by commas."""
    m = re.search("dependenc(y|ies):(.*)", pr.body, re.I)
    if not m: return True
    deps = [d.strip() for d in m.group(2).strip().split(",") if d.strip()]
    for d in deps:
        dep_pr_m = re.match("([0-9]+)@(.*)", d)
        if not dep_pr_m:
            report_error(pr, "Could not parse dependency: %s" % d, False)
            return False
        dep_pr_no = dep_pr_m.group(1)
        dep_pr_rep = dep_pr_m.group(2)
        if dep_pr_rep not in rep_names:
            report_error(pr, "Dependency on unknown depository: %s" % dep_pr_rep, False)
            return False
        dep_pr = None
        try:
            dep_pr = org.get_repo(dep_pr_rep).get_pull(int(dep_pr_no))
        except:
            report_error(pr, "Could not find dependency: %s" % d, False)
            return False
        if not hasattr(dep_pr, "merged_at"):
            log("DEPENDENCY NOT MERGED: %s for %s/%d" % (d, rep_name, pr.number))
            return False
    return True

def succeeded_comment(comments):
    for c in comments:
        first_line = c.body.split("\n")[0]
        if (first_line.find("Build succeeded.") != -1):
            return True
    return False

def should_rebuild(pr, comments):
    """Checks the pull requests and its comments to see whether the pull request
    has succeeded the last time, and whether the refs have changed."""
    rep_name = pr.base.repo.name
    if not dependencies_satisfied(pr, rep_name):
        return False, False, False
    # approve if no existing bot comments
    bot_comments = [c for c in comments if c.user.login == bot_name]
    if not bot_comments:
        log("NO COMMENTS: %s/%d" % (rep_name, pr.number))
        return False, True, False # "last build not succeeded", "refs changed"
    # otherwise, parse last bot's comment, and check for ref changes
    succeeded = succeeded_comment(bot_comments)
    last_bot_comment = bot_comments[-1]
    first_line = last_bot_comment.body.split("\n")[0]
    refs = re.findall("\S+?@\w+", first_line, re.U)
    last_pr_ref = refs[0]
    last_branch_ref = refs[1]
    current_pr_ref = get_pr_ref(pr)
    branch = pr.base.ref
    current_branch_ref = get_branch_ref(rep_name, branch)
    changed = last_pr_ref != current_pr_ref or last_branch_ref != current_branch_ref
    if changed: log("REFS CHANGED: %s/%d" % (rep_name, pr.number))
    return succeeded, False, changed

def report_error(pr, ex_msg, show_log):
    """Report an error regarding the given pull request with the given
    message. The message is reported on standard output and GitHub."""
    rep_name = pr.base.repo.name
    pr_ref = get_pr_ref(pr)
    branch = pr.base.ref
    branch_ref = get_branch_ref(rep_name, branch)
    prefix = bot_msg_prefix(pr_ref, branch_ref)
    msg = "%s Merge and build failed.\n%s" % (prefix, ex_msg)
    if show_log:
        msg += "\nError log:"
        f = open(log_path)
        lines = f.readlines()
        f.close()
        linesToPrint = min(20, len(lines))
        firstLineToPrint = len(lines) - linesToPrint
        for i in range(firstLineToPrint, firstLineToPrint + linesToPrint):
            msg += "\n    %s" % lines[i].rstrip()
    print_msg(pr, msg)
    if active:
        issue = pr.base.repo.get_issue(pr.number)
        issue.create_comment(msg.replace('%','_')) # PyGithub doesn't like % character.

def bot_msg_prefix(pr_ref, branch_ref):
    return "### %s &#8658; %s:" % (pr_ref, branch_ref)

def print_msg(pr, msg):
    """Print the given message together with a unique identifier of the given
    pull request to standard output."""
    print "============================="
    print "Pull request: %s\n%s" % (pr.html_url, msg)
    print "============================="

def execute(path, cmd):
    """Execute the given command in the given path."""
    cwd = os.getcwd()
    os.chdir(path)
    log("Executing '%s' in '%s' ..." % (cmd, path))
    retcode = os.system("GIT_USER=%s %s 2>&1 >> %s" % (bot_name, cmd, log_path))
    os.chdir(cwd)
    return retcode

def execute_and_return(path, cmd):
    """Execute the given command in the given path, and return whatever was
    printed to stdout."""
    cwd = os.getcwd()
    os.chdir(path)
    log("Executing '%s' in '%s' ..." % (cmd, path))
    output = os.popen("GIT_USER=%s %s" % (bot_name, cmd)).read()
    os.chdir(cwd)
    return output

class BuildError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

def execute_and_report(path, cmd):
    """Execute the given command in the given path, raising an exception for a
    non-zero return code."""
    if execute(path, cmd) != 0:
        raise BuildError("Failed when executing:\n    %s" % cmd)

class MergeError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

class VerificationError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

def obtain_normalised_shas(rep_dir, src_files, ref):
    shas = dict()
    execute_and_report(rep_dir, "git checkout -B %s %s" % (tmp_branch, ref))
    for src_file in src_files:
        camlp4_cmd = "camlp4 -parser o -printer o -no_comments %s | md5sum" % src_file
        shas[src_file] = execute_and_return(rep_dir, camlp4_cmd).split()[0]
    execute_and_report(rep_dir, "git checkout master")
    return shas

def verify_whitespace_changes(rep_dir, pr):
    log("Verifying whitespace changes..")
    checked = False
    execute_and_report(rep_dir, "git checkout master")
    prev = pr.base.sha
    log_range = "%s..%s" % (pr.base.sha, pr.head.sha)
    log_cmd = "git log --reverse --pretty=oneline %s" % log_range
    out = execute_and_return(rep_dir, log_cmd)
    for line in out.split("\n"):
        if line == "": continue
        parts = line.split(" ", 1)
        curr = parts[0]
        comment = parts[1]
        if re.match("\[?(indentation|whitespace)", comment, re.I):
            files_cmd = "git show --pretty=\"format:\" --name-only %s" % curr
            out = execute_and_return(rep_dir, files_cmd)
            src_files = [f for f in out.split("\n") if re.match(".+\.ml[i]?", f)]
            prev_shas = obtain_normalised_shas(rep_dir, src_files, prev)
            curr_shas = obtain_normalised_shas(rep_dir, src_files, curr)
            for src_file in src_files:
                if prev_shas[src_file] != curr_shas[src_file]:
                    ref = get_pr_ref(pr, curr)
                    msg = "Whitespace check failed for %s at %s." % (src_file, ref)
                    raise VerificationError(msg)
            checked = True
        prev = curr
    return checked

def search_title_for_key(pr):
    m = re.match("\[?(ca-[0-9]+)", pr.title, re.I)
    if m: return m.group(1)

def closeTicket(pr, key):
    j = jira.Jira(settings.jira_url, settings.jira_username, settings.jira_password)
    i = j.getIssue(key)
    if active:
        link = "[A patch|%s]" % pr.html_url
        i.addComment("%s that fixes this issue has been merged into trunk." % link)
        i.resolve("Fixed")

def create_jira_issue(pr):
    log("Creating a merge request ticket for Ben Chalmers")
    msg = "You can merge the following pull request: %s" % pr.html_url
    jira_auth = jira.Jira(settings.jira_url, settings.jira_username, settings.jira_password)
    ticket = jira_auth.createIssue(project='CA', summary='Merge request for Tampa', type='Merge Request',
                                 priority='Major', description=msg, assignee=settings.jira_assignee)
    origin = search_title_for_key(pr)
    if origin: ticket.linkIssue(origin,"contains")
    return ticket.getKey()

def process_pull_request(pr, rebuild_required, merge, ticket, send_notification):
    """If a rebuild is required, try building the system with the changesets
    from the given pull request. If the build succeeds and the merge has been
    requested, merge the pull request with the main repository. Also close the
    Jira ticket if merged and a ticket is specified."""
    if not rebuild_required and not merge:
        log("Invalid call: rebuild_required=False, merge=False")
        return
    rep_name = pr.base.repo.name
    rep_path = "%s/%s" % (org_name, rep_name)
    user = pr.user.login # user doing the pull request
    owner = pr.head.repo.owner.login # owner of the pull request's repository
    log("Processing pull request %s/%d .." % (rep_path, pr.number))
    component_name = rep_names[rep_name]
    rep_dir = "%s/myrepos/%s" % (build_path, rep_name)
    branch = pr.base.ref
    branch_sha = get_cached_branch_sha(rep_name, branch)
    internal_branch = branch_whitelist[branch]
    build_rep = "%s/%s/build.hg" % (build_rep_prefix, internal_branch)
    path_cmds = [
        (settings.builds_path, "sudo rm -rf %s %s" % (build_dir, log_file)),
        (settings.builds_path, "hg clone %s %s" % (build_rep, build_dir)),
        (build_path, "make manifest-latest"),
        ]
    for path, cmd in path_cmds: execute_and_report(path, cmd)
#    for c in rep_names.itervalues(): execute_and_report(build_path, "make %s-myclone" % c)
    execute_and_report(build_path, "make %s-myclone" % component_name)
    merge_msg = "Merge pull request #%d from %s/%s" % (pr.number,owner,pr.head.ref)
    path_cmds = [
        (rep_dir, "git config user.name %s" % bot_name),
        (rep_dir, "git config user.email %s" % settings.bot_email),
        (rep_dir, "git checkout master"),
        (rep_dir, "git remote add %s git://github.com/%s/%s.git" % (user, owner, rep_name)),
        (rep_dir, "git fetch %s" % user),
        (rep_dir, "git merge -m \"%s\" %s" % (merge_msg,pr.head.sha)),
        ]
    for path, cmd in path_cmds: execute_and_report(path, cmd)
    pr_ref = get_pr_ref(pr)
    branch_ref = get_branch_ref(rep_name, branch)
    msg = bot_msg_prefix(pr_ref, branch_ref)
    if verify_whitespace_changes(rep_dir, pr):
        msg += " Whitespace changes verified."
    if rebuild_required:
        execute_and_report(build_path, "make %s-build" % component_name)
        if component_name != "api":
            execute_and_report(build_path, "make api-build")
    msg += " Build succeeded."
    if merge:
        fresh_branch_sha = get_fresh_branch_sha(rep_name, branch)
        if fresh_branch_sha != branch_sha:
            fresh_branch_ref = get_branch_ref(rep_name, branch, fresh_branch_sha)
            raise MergeError("Branch %s updated since to %s." % (branch, fresh_branch_ref))
        fresh_pr = org.get_repo(rep_name).get_pull(pr.number)
        if fresh_pr.state != "open":
            raise MergeError("Pull request %s no longer 'open'." % rep_path)
        if fresh_pr.head.sha != pr.head.sha:
            fresh_pr_ref = get_pr_ref(fresh_pr)
            raise MergeError("Pull request %s modified since to %s." % (rep_path, fresh_pr_ref))
        rep_url = "git@github-xen-git:%s.git" % rep_path
        path_cmds = [
            (rep_dir, "git remote add %s %s" % (org_name, rep_url)),
            (rep_dir, "git push %s master:%s" % (org_name, branch)),
            ]
        if active:
            for path, cmd in path_cmds: execute_and_report(path, cmd)
        msg += " Pull request merged." 
        '''
        if settings.jira_url and ticket:
            ticket_ref = "[{0}](http://jira/browse/{0})".format(ticket)
            try:
                closeTicket(pr, ticket)
                msg += " Closed ticket %s." % ticket_ref
            except Exception, e:
                traceback.print_exc()
                msg += " Failed to close ticket %s (reason: %s)." % (ticket_ref, e)
        '''
        print_msg(pr, msg)
        if active:
            pr.base.repo.get_issue(pr.number).create_comment(msg.replace('%','_'))
            pr.base.repo.get_issue(pr.number).edit(state="closed")
        log("Allowing for local/GitHub repos re-sync. Sleeping for %ds." % resync_sleep)
        time.sleep(resync_sleep)
    else:
        msg += " Can merge pull request."
        if (branch == "tampa" and send_notification):
            ca_ticket = create_jira_issue(pr)
            msg += "\nJira ticket %s" % ca_ticket
        print_msg(pr, msg)
        if active: pr.base.repo.get_issue(pr.number).create_comment(msg.replace('%','_'))

def get_fresh_branch_sha(rep_name, branch):
    """Obtain SHA of the last commit of the specified branch of the specified
    repository."""
    repo = org.get_repo(rep_name)
    branch_sha = [br.commit.sha for br in repo.get_branches() if br.name == branch][0]
    return branch_sha

def get_cached_branch_sha(rep_name, branch):
    """Obtain SHA of the last commit of the specified branch of the specified
    repository. The results are cached."""
    global branch_sha_cache
    rep_path = "%s/%s" % (org_name, rep_name)
    try:
        branch_sha = branch_sha_cache[(rep_path, branch)]
    except KeyError:
        branch_sha = get_fresh_branch_sha(rep_name, branch)
        branch_sha_cache[(rep_path, branch)] = branch_sha
    return branch_sha

def get_branch_ref(rep_name, branch, branch_sha=None):
    if not branch_sha: branch_sha = get_cached_branch_sha(rep_name, branch)
    return "%s/%s@%s" % (org_name, rep_name, branch_sha)

def get_pr_ref(pr, ref=None):
    if ref == None: ref = pr.head.sha
    return "%s/%s@%s" % (pr.head.repo.owner.login,
                         pr.head.repo.name, ref)

def clear_state():
    """Clears any global state due to the processing of pull requests."""
    global branch_sha_cache
    branch_sha_cache = {}

def log(msg):
    print "[%s] %s" % (time.ctime(), msg)

if __name__ == "__main__":
    """Continually obtain pull requests, and process them. If there are no pull
    requests to process, wait for a while."""
    run = 0
    while True:
        try:
            clear_state()
            with Watchdog(fetch_timeout):
                if run % 10 == 0:
                    run = 0
                    refresh_privileges()
                pr, rebuild, merge, ticket, send_notification = get_next_pull_request()
            if pr:
                with Watchdog(build_timeout):
                    process_pull_request(pr, rebuild, merge, ticket, send_notification)
            else:
                log("No appropriate pull requests found.")
            log("Sleeping for %ds." % short_sleep)
            time.sleep(short_sleep)
            run += 1
        except BuildError as ex:
            report_error(pr, ex.message, True)
        except MergeError as ex:
            report_error(pr, ex.message, False)
        except VerificationError as ex:
            report_error(pr, ex.message, False)
        except Watchdog:
            traceback.print_exc()
            log("Operation timed out. Sleeping for %ds." % long_sleep)
            time.sleep(long_sleep)
        except:
            traceback.print_exc()
            log("Unexpected error occurred. Sleeping for %ds." % long_sleep)
            time.sleep(long_sleep)
