import os
from datetime import datetime
import subprocess
import time
import logging
from threading import Thread
from github import Github
from github_archive.logger import Logger

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
ORG_LIST = os.getenv('GITHUB_ARCHIVE_ORGS', '')
ORGS = ORG_LIST.split(',')
GITHUB_ARCHIVE_LOCATION = os.path.expanduser(
    os.getenv('GITHUB_ARCHIVE_LOCATION', '~/github-archive')
)
LOGGER = logging.getLogger(__name__)
USER = Github(GITHUB_TOKEN).get_user()
# TODO: Add user/password authentication (will need to pull from non-ssh url)
# BUFFER is the time in between each request - helps with rate limiting
BUFFER = float(os.getenv('GITHUB_ARCHIVE_BUFFER', 0.1))
# GIT_TIMEOUT is the number of seconds before a git operation will timeout
GIT_TIMEOUT = int(os.getenv('GITHUB_ARCHIVE_TIMEOUT', 180))


class GithubArchive():
    @staticmethod
    def run(user_clone=False, user_pull=False, gists_clone=False, gists_pull=False, orgs_clone=False,
            orgs_pull=False, branch=None):
        """Run the tool based on the arguments passed
        """
        GithubArchive.initialize_project()
        Logger._setup_logging(LOGGER)
        clone = 'clone'
        pull = 'pull'
        LOGGER.info('# GitHub Archive started...\n')
        start_time = datetime.now()
        results = {"org": {},"personal": {}, "gist":{}}

        if user_clone or user_pull:
            repos = GithubArchive.get_repos()

        # Iterate over each personal repo and concurrently clone it
        if user_clone is True:
            LOGGER.info('# Cloning personal repos...\n')
            results["personal"]["clone"] = GithubArchive.determine_repo_context(repos, 'user', clone, branch)
        else:
            LOGGER.info('# Skipping cloning user repos...\n')

        # Iterate over each personal repo and concurrently pull it
        if user_pull is True:
            LOGGER.info('# Pulling personal repos...\n')
            results["personal"]["pull"] =GithubArchive.determine_repo_context(repos, 'user', pull, branch)
        else:
            LOGGER.info('# Skipping pulling user repos...\n')

        # Check if org list is populated
        if ORG_LIST != '':
            if orgs_clone or orgs_pull:
                org_repos = GithubArchive.get_all_org_repos()
            # Iterate over each org repo and concurrently clone it
            if orgs_clone is True:
                LOGGER.info('# Cloning org repos...\n')
                results["org"]["clone"] = GithubArchive.determine_repo_context(org_repos, 'orgs', clone, branch)
            else:
                LOGGER.info('# Skipping cloning org repos...\n')
            # Iterate over each org repo and concurrently pull it
            if orgs_pull is True:
                LOGGER.info('# Pulling org repos...\n')
                results["org"]["pull"] = GithubArchive.determine_repo_context(org_repos, 'orgs', pull, branch)
            else:
                LOGGER.info('# Skipping cloning org repos...\n')
        else:
            LOGGER.info('# Skipping org repos, no orgs configured...\n')

        if gists_clone or gists_pull:
            gists = GithubArchive.get_gists()

        # Iterate over each gist and concurrently clone it
        if gists_clone is True:
            LOGGER.info('# Cloning gists...\n')
            results["gist"]["clone"] = GithubArchive.iterate_gists(gists, clone)
        else:
            LOGGER.info('# Skipping cloning gists...\n')

        # Iterate over each gist and concurrently pull it
        if gists_pull is True:
            LOGGER.info('# Pulling gists...\n')
            results["gist"]["pull"] = GithubArchive.iterate_gists(gists, pull)
        else:
            LOGGER.info('# Skipping pulling gists...\n')

        execution_time = f'Execution time: {datetime.now() - start_time}.'
        finish_message = f'GitHub Archive complete! {execution_time}\n'
        LOGGER.info(finish_message)
        GithubArchive.summary(results)

    @staticmethod
    def count_results(results):
        count = 0
        numbers = {
            "error": 0,
            "skip": 0,
            "success": 0,
            "timeout": 0,
            "fail": 0,
        }
        for repo in results:
            count += 1
            numbers[repo["result"]] += 1
        LOGGER.info(f"| total {count:>4d} | skip {numbers['skip']:>4d} | success {numbers['success']:>4d} | timeout {numbers['timeout']:>4d} | fail {numbers['fail']:>4d} | error {numbers['error']:>4d} |")

    @staticmethod
    def summary(results):
        LOGGER.info("==== Summary ====")
        if results["org"].get("clone",[]):
            LOGGER.info("Organization repos [clone]")
            GithubArchive.count_results(results["org"].get("clone",[]))
        if results["org"].get("pull",[]):
            LOGGER.info("Organization repos [pull]")
            GithubArchive.count_results(results["org"].get("pull",[]))
        if results["personal"].get("pull",[]):
            LOGGER.info("Personal repos [pull]")
            GithubArchive.count_results(results["personal"].get("clone",[]))
        if results["personal"].get("pull",[]):
            LOGGER.info("Personal repos [pull]")
            GithubArchive.count_results(results["personal"].get("pull",[]))
        if results["gist"].get("clone",[]):
            LOGGER.info("Gists [clone]")
            GithubArchive.count_results(results["gist"].get("clone",[]))
        if results["gist"].get("pull",[]):
            LOGGER.info("Gists [pull]")
            GithubArchive.count_results(results["gist"].get("pull",[]))



    @staticmethod
    def initialize_project():
        """Initialize the tool and ensure everything is
        in order before running any logic
        """
        if not GITHUB_TOKEN:
            message = 'GITHUB_TOKEN must be present to run github-archive.'
            LOGGER.critical(message)
            raise ValueError(message)
        if not os.path.exists(GITHUB_ARCHIVE_LOCATION):
            os.makedirs(os.path.join(GITHUB_ARCHIVE_LOCATION, 'repos'))
        if not os.path.exists(GITHUB_ARCHIVE_LOCATION):
            os.makedirs(os.path.join(GITHUB_ARCHIVE_LOCATION, 'gists'))

    @staticmethod
    def get_repos():
        """Retrieve repos of a given user
        """
        repos = USER.get_repos()
        return repos

    @staticmethod
    def get_all_org_repos():
        """Retrieve repos of all orgs in the orgs list
        """
        all_org_repos = []
        for org in ORGS:
            all_org_repos.append(Github(GITHUB_TOKEN).get_organization(org.strip()).get_repos())
        return all_org_repos

    @staticmethod
    def get_gists():
        """Retrieve gists of a given user
        """
        gists = USER.get_gists()
        return gists

    @staticmethod
    def determine_repo_context(repos, context, operation, branch=None):
        """Determine if a repo is from a user or org
        and route the logic accordingly
        """
        if context == 'orgs':
            for single_org_repos in repos:
                result = GithubArchive.iterate_repos(single_org_repos, context, operation, branch)
        elif context == 'user':
            result = GithubArchive.iterate_repos(repos, context, operation, branch)
        else:
            message = f'Could not determine what action to take with {context}.'
            LOGGER.error(message)
            raise ValueError(message)
        return result

    @staticmethod
    def iterate_repos(repos, context, operation, branch=None):
        """Iterate over each repository
        """
        thread_list = []
        results = []
        for repo in repos:
            # We check the owner name here to ensure that we only iterate
            # through the user's personal repos which will exclude orgs
            # that can instead be iterated by passing the "--clone_orgs"
            # or "--pull_orgs" flags to allow for granular control
            if repo.owner.name != USER.name and context == 'user':
                continue
            else:
                time.sleep(BUFFER)
                path = os.path.join(GITHUB_ARCHIVE_LOCATION, 'repos', repo.owner.login, repo.name)
                repo_thread = Thread(
                    target=GithubArchive.archive_repo,
                    args=(
                        context,
                        repo,
                        path,
                        operation,
                        results,
                        branch,
                    )
                )
                thread_list.append(repo_thread)
                repo_thread.start()
        for thread in thread_list:
            thread.join()
        return results

    @staticmethod
    def iterate_gists(gists, operation):
        """Iterate over each gist
        """
        thread_list = []
        results = []
        for gist in gists:
            time.sleep(BUFFER)
            path = os.path.join(GITHUB_ARCHIVE_LOCATION, 'gists', gist.id)
            repo_thread = Thread(
                target=GithubArchive.archive_gist,
                args=(
                    gist,
                    path,
                    results,
                    operation,
                )
            )
            thread_list.append(repo_thread)
            repo_thread.start()
        for thread in thread_list:
            thread.join()
        return results

    @staticmethod
    def archive_repo(context, repo, path, operation, results, branch=None):
        """Clone and pull repos based on the operation passed
        """
        branch_flag = f'--branch={branch}' if branch else ''  # Only switch default branch if the user explicitly asks
        if os.path.exists(path) and operation == 'clone':
            LOGGER.info(f'Repo: {repo.name} already cloned, skipping clone operation.')
            results.append({"context": context, "repo": repo.name, "result": "skip"})
        else:
            if operation == 'clone':
                command = (f'git clone {branch_flag} {repo.ssh_url} {path}')
            elif operation == 'pull':
                command = f'cd {path} && git pull --rebase'
            else:
                message = f'Could not determine what action to take with {repo.name} based on {operation}.'
                LOGGER.error(message)
                results.append({"context": context, "repo": repo.name, "result": "error"})
                raise ValueError(message)

            try:
                subprocess.run(
                    command,
                    stdin=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    shell=True,
                    check=True,
                    timeout=int(GIT_TIMEOUT)
                )
                LOGGER.info(f'Repo: {repo.name} {operation} success!')
                results.append({"context": context, "repo": repo.name, "result": "success"})
            except subprocess.TimeoutExpired:
                LOGGER.error(f'Git operation timed out archiving {repo.name}.')
                results.append({"context": context, "repo": repo.name, "result": "timeout"})
            except subprocess.CalledProcessError as error:
                LOGGER.error(f'Failed to {operation} {repo.name}\n{error}')
                results.append({"context": context, "repo": repo.name, "result": "fail"})

    @staticmethod
    def archive_gist(gist, path, operation, results):
        """Clone and pull gists based on the operation passed
        """
        if os.path.exists(path) and operation == 'clone':
            LOGGER.info(f'Gist: {gist.id} already cloned, skipping clone operation.')
            results.append({"gist": gist.id, "result": "skip"})
        else:
            if operation == 'clone':
                command = f'git clone {gist.html_url} {path}'
            elif operation == 'pull':
                command = f'cd {path} && git pull --rebase'
            else:
                message = f'Could not determine what action to take with {gist.id} based on {operation}.'
                LOGGER.error(message)
                results.append({"gist": gist.id, "result": "error"})
                raise ValueError(message)

            try:
                subprocess.run(
                    command,
                    stdin=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    shell=True,
                    check=True,
                    timeout=int(GIT_TIMEOUT)
                )
                LOGGER.info(f'Gist: {gist.id} {operation} success!')
                results.append({"gist": gist.id, "result": "success"})
            except subprocess.TimeoutExpired:
                LOGGER.error(f'Git operation timed out archiving {gist.id}.')
                results.append({"gist": gist.id, "result": "timeout"})
            except subprocess.CalledProcessError as error:
                LOGGER.error(f'Failed to {operation} {gist.id}\n{error}')
                results.append({"gist": gist.id, "result": "fail"})
