import os
import re
import time
from datetime import datetime
from pathlib import Path, PurePosixPath

try:
    import git

    ANY_GIT_ERROR = [
        git.exc.ODBError,
        git.exc.GitError,
        git.exc.InvalidGitRepositoryError,
        git.exc.GitCommandNotFound,
    ]
except ImportError:
    git = None
    ANY_GIT_ERROR = []

import pathspec

from aider import prompts, utils

from .dump import dump  # noqa: F401

ANY_GIT_ERROR += [
    OSError,
    IndexError,
    BufferError,
    TypeError,
    ValueError,
    AttributeError,
    AssertionError,
    TimeoutError,
]
ANY_GIT_ERROR = tuple(ANY_GIT_ERROR)


class GitRepo:
    repo = None
    aider_ignore_file = None
    aider_ignore_spec = None
    aider_ignore_ts = 0
    aider_ignore_last_check = 0
    subtree_only = False
    ignore_file_cache = {}
    git_repo_error = None
    primary_branch = None # Typically 'main' or 'master'
    develop_branch = None # Typically 'develop'

    def __init__(
        self,
        io,
        fnames,
        git_dname,
        aider_ignore_file=None,
        models=None,
        attribute_author=True,
        attribute_committer=True,
        attribute_commit_message_author=False,
        attribute_commit_message_committer=False,
        commit_prompt=None,
        subtree_only=False,
        git_commit_verify=True,
        config=None,
        primary_branch_name="main", # Default primary branch
    ):
        self.io = io
        self.models = models
        self.config = config
        # primary_branch_name is the user's preferred *production* branch name
        self.primary_branch_name = primary_branch_name

        self.normalized_path = {}
        self.tree_files = {}

        self.attribute_author = attribute_author
        self.attribute_committer = attribute_committer
        self.attribute_commit_message_author = attribute_commit_message_author
        self.attribute_commit_message_committer = attribute_commit_message_committer
        self.commit_prompt = commit_prompt
        self.subtree_only = subtree_only
        self.git_commit_verify = git_commit_verify
        self.ignore_file_cache = {}

        if git_dname:
            check_fnames = [git_dname]
        elif fnames:
            check_fnames = fnames
        else:
            check_fnames = ["."]

        repo_paths = []
        for fname in check_fnames:
            fname = Path(fname)
            fname = fname.resolve()

            if not fname.exists() and fname.parent.exists():
                fname = fname.parent

            try:
                repo_path = git.Repo(fname, search_parent_directories=True).working_dir
                repo_path = utils.safe_abs_path(repo_path)
                repo_paths.append(repo_path)
            except ANY_GIT_ERROR:
                pass

        num_repos = len(set(repo_paths))

        if num_repos == 0:
            raise FileNotFoundError
        if num_repos > 1:
            self.io.tool_error("Files are in different git repos.")
            raise FileNotFoundError

        # https://github.com/gitpython-developers/GitPython/issues/427
        self.repo = git.Repo(repo_paths.pop(), odbt=git.GitDB)
        self.root = utils.safe_abs_path(self.repo.working_tree_dir)

        # Determine primary and develop branches based on Git Flow conventions
        self._determine_git_flow_branches()

        if aider_ignore_file:
            self.aider_ignore_file = Path(aider_ignore_file)

    def _determine_git_flow_branches(self):
        """Sets self.primary_branch and self.develop_branch based on common names."""
        try:
            branches = [branch.name for branch in self.repo.branches]
            current_branch = self.get_current_branch() # Get current branch early

            # Determine Primary Branch (main/master)
            if self.primary_branch_name in branches:
                self.primary_branch = self.primary_branch_name
            elif "main" in branches:
                self.primary_branch = "main"
            elif "master" in branches: # Support 'master' as well
                self.primary_branch = "master"
            else:
                # Fallback logic for primary
                if "develop" in branches:
                    self.primary_branch = "develop" # If no main/master, develop might be primary
                    self.io.tool_warning("No 'main' or 'master' branch found. Using 'develop' as primary.")
                else:
                    self.primary_branch = current_branch # Last resort
                    self.io.tool_warning(f"No 'main', 'master', or 'develop' branch found. Using current branch '{current_branch}' as primary.")

            # Determine Develop Branch (develop)
            if "develop" in branches:
                self.develop_branch = "develop"
            else:
                # If no 'develop', fallback to the primary branch as the development base
                self.develop_branch = self.primary_branch
                if self.primary_branch != "develop": # Avoid redundant message
                    self.io.tool_warning(f"No 'develop' branch found. Using primary branch '{self.primary_branch}' as the development base.")

            self.io.tool_output(f"Identified Primary Branch: {self.primary_branch}")
            self.io.tool_output(f"Identified Develop Branch: {self.develop_branch}")

        except Exception as e:
            self.io.tool_error(f"Error determining Git Flow branches: {e}. Falling back to defaults.")
            # Fallback values
            self.primary_branch = self.primary_branch_name or "main"
            self.develop_branch = "develop" if "develop" in self.repo.branches else self.primary_branch

    def commit(self, fnames=None, context=None, message=None, aider_edits=False):
        if not fnames and not self.repo.is_dirty():
            return

        diffs = self.get_diffs(fnames)
        if not diffs:
            return

        if message:
            commit_message = message
        else:
            commit_message = self.get_commit_message(diffs, context)

        if aider_edits and self.attribute_commit_message_author:
            commit_message = "aider: " + commit_message
        elif self.attribute_commit_message_committer:
            commit_message = "aider: " + commit_message

        if not commit_message:
            commit_message = "(no commit message provided)"

        full_commit_message = commit_message
        # if context:
        #    full_commit_message += "\n\n# Aider chat conversation:\n\n" + context

        cmd = ["-m", full_commit_message]
        if not self.git_commit_verify:
            cmd.append("--no-verify")
        if fnames:
            fnames = [str(self.abs_root_path(fn)) for fn in fnames]
            for fname in fnames:
                try:
                    self.repo.git.add(fname)
                except ANY_GIT_ERROR as err:
                    self.io.tool_error(f"Unable to add {fname}: {err}")
            cmd += ["--"] + fnames
        else:
            cmd += ["-a"]

        original_user_name = self.repo.git.config("--get", "user.name")
        original_committer_name_env = os.environ.get("GIT_COMMITTER_NAME")
        committer_name = f"{original_user_name} (aider)"

        if self.attribute_committer:
            os.environ["GIT_COMMITTER_NAME"] = committer_name

        if aider_edits and self.attribute_author:
            original_author_name_env = os.environ.get("GIT_AUTHOR_NAME")
            os.environ["GIT_AUTHOR_NAME"] = committer_name

        try:
            self.repo.git.commit(cmd)
            commit_hash = self.get_head_commit_sha(short=True)
            self.io.tool_output(f"Commit {commit_hash} {commit_message}", bold=True)
            return commit_hash, commit_message
        except ANY_GIT_ERROR as err:
            self.io.tool_error(f"Unable to commit: {err}")
        finally:
            # Restore the env

            if self.attribute_committer:
                if original_committer_name_env is not None:
                    os.environ["GIT_COMMITTER_NAME"] = original_committer_name_env
                else:
                    del os.environ["GIT_COMMITTER_NAME"]

            if aider_edits and self.attribute_author:
                if original_author_name_env is not None:
                    os.environ["GIT_AUTHOR_NAME"] = original_author_name_env
                else:
                    del os.environ["GIT_AUTHOR_NAME"]

    def get_rel_repo_dir(self):
        try:
            return os.path.relpath(self.repo.git_dir, os.getcwd())
        except (ValueError, OSError):
            return self.repo.git_dir

    def get_commit_message(self, diffs, context):
        diffs = "# Diffs:\n" + diffs

        content = ""
        if context:
            content += context + "\n"
        content += diffs

        system_content = self.commit_prompt or prompts.commit_system
        messages = [
            dict(role="system", content=system_content),
            dict(role="user", content=content),
        ]

        commit_message = None
        for model in self.models:
            num_tokens = model.token_count(messages)
            max_tokens = model.info.get("max_input_tokens") or 0
            if max_tokens and num_tokens > max_tokens:
                continue
            commit_message = model.simple_send_with_retries(messages)
            if commit_message:
                break

        if not commit_message:
            self.io.tool_error("Failed to generate commit message!")
            return

        commit_message = commit_message.strip()
        if commit_message and commit_message[0] == '"' and commit_message[-1] == '"':
            commit_message = commit_message[1:-1].strip()

        return commit_message

    def get_diffs(self, fnames=None):
        # We always want diffs of index and working dir

        current_branch_has_commits = False
        try:
            active_branch = self.repo.active_branch
            try:
                commits = self.repo.iter_commits(active_branch)
                current_branch_has_commits = any(commits)
            except ANY_GIT_ERROR:
                pass
        except (TypeError,) + ANY_GIT_ERROR:
            pass

        if not fnames:
            fnames = []

        diffs = ""
        for fname in fnames:
            if not self.path_in_repo(fname):
                diffs += f"Added {fname}\n"

        try:
            if current_branch_has_commits:
                args = ["HEAD", "--"] + list(fnames)
                diffs += self.repo.git.diff(*args)
                return diffs

            wd_args = ["--"] + list(fnames)
            index_args = ["--cached"] + wd_args

            diffs += self.repo.git.diff(*index_args)
            diffs += self.repo.git.diff(*wd_args)

            return diffs
        except ANY_GIT_ERROR as err:
            self.io.tool_error(f"Unable to diff: {err}")

    def diff_commits(self, pretty, from_commit, to_commit):
        args = []
        if pretty:
            args += ["--color"]
        else:
            args += ["--color=never"]

        args += [from_commit, to_commit]
        diffs = self.repo.git.diff(*args)

        return diffs

    def get_tracked_files(self):
        if not self.repo:
            return []

        try:
            commit = self.repo.head.commit
        except ValueError:
            commit = None
        except ANY_GIT_ERROR as err:
            self.git_repo_error = err
            self.io.tool_error(f"Unable to list files in git repo: {err}")
            self.io.tool_output("Is your git repo corrupted?")
            return []

        files = set()
        if commit:
            if commit in self.tree_files:
                files = self.tree_files[commit]
            else:
                try:
                    iterator = commit.tree.traverse()
                    blob = None  # Initialize blob
                    while True:
                        try:
                            blob = next(iterator)
                            if blob.type == "blob":  # blob is a file
                                files.add(blob.path)
                        except IndexError:
                            # Handle potential index error during tree traversal
                            # without relying on potentially unassigned 'blob'
                            self.io.tool_warning(
                                "GitRepo: Index error encountered while reading git tree object."
                                " Skipping."
                            )
                            continue
                        except StopIteration:
                            break
                except ANY_GIT_ERROR as err:
                    self.git_repo_error = err
                    self.io.tool_error(f"Unable to list files in git repo: {err}")
                    self.io.tool_output("Is your git repo corrupted?")
                    return []
                files = set(self.normalize_path(path) for path in files)
                self.tree_files[commit] = set(files)

        # Add staged files
        index = self.repo.index
        try:
            staged_files = [path for path, _ in index.entries.keys()]
            files.update(self.normalize_path(path) for path in staged_files)
        except ANY_GIT_ERROR as err:
            self.io.tool_error(f"Unable to read staged files: {err}")

        res = [fname for fname in files if not self.ignored_file(fname)]

        return res

    def normalize_path(self, path):
        orig_path = path
        res = self.normalized_path.get(orig_path)
        if res:
            return res

        path = str(Path(PurePosixPath((Path(self.root) / path).relative_to(self.root))))
        self.normalized_path[orig_path] = path
        return path

    def refresh_aider_ignore(self):
        if not self.aider_ignore_file:
            return

        current_time = time.time()
        if current_time - self.aider_ignore_last_check < 1:
            return

        self.aider_ignore_last_check = current_time

        if not self.aider_ignore_file.is_file():
            return

        mtime = self.aider_ignore_file.stat().st_mtime
        if mtime != self.aider_ignore_ts:
            self.aider_ignore_ts = mtime
            self.ignore_file_cache = {}
            lines = self.aider_ignore_file.read_text().splitlines()
            self.aider_ignore_spec = pathspec.PathSpec.from_lines(
                pathspec.patterns.GitWildMatchPattern,
                lines,
            )

    def git_ignored_file(self, path):
        if not self.repo:
            return
        try:
            if self.repo.ignored(path):
                return True
        except ANY_GIT_ERROR:
            return False

    def ignored_file(self, fname):
        self.refresh_aider_ignore()

        if fname in self.ignore_file_cache:
            return self.ignore_file_cache[fname]

        result = self.ignored_file_raw(fname)
        self.ignore_file_cache[fname] = result
        return result

    def ignored_file_raw(self, fname):
        if self.subtree_only:
            try:
                fname_path = Path(self.normalize_path(fname))
                cwd_path = Path.cwd().resolve().relative_to(Path(self.root).resolve())
            except ValueError:
                # Issue #1524
                # ValueError: 'C:\\dev\\squid-certbot' is not in the subpath of
                # 'C:\\dev\\squid-certbot'
                # Clearly, fname is not under cwd... so ignore it
                return True

            if cwd_path not in fname_path.parents and fname_path != cwd_path:
                return True

        if not self.aider_ignore_file or not self.aider_ignore_file.is_file():
            return False

        try:
            fname = self.normalize_path(fname)
        except ValueError:
            return True

        return self.aider_ignore_spec.match_file(fname)

    def path_in_repo(self, path):
        if not self.repo:
            return
        if not path:
            return

        tracked_files = set(self.get_tracked_files())
        return self.normalize_path(path) in tracked_files

    def abs_root_path(self, path):
        res = Path(self.root) / path
        return utils.safe_abs_path(res)

    def get_dirty_files(self):
        """
        Returns a list of all files which are dirty (not committed), either staged or in the working
        directory.
        """
        dirty_files = set()

        # Get staged files
        staged_files = self.repo.git.diff("--name-only", "--cached").splitlines()
        dirty_files.update(staged_files)

        # Get unstaged files
        unstaged_files = self.repo.git.diff("--name-only").splitlines()
        dirty_files.update(unstaged_files)

        return list(dirty_files)

    def is_dirty(self, path=None):
        if path and not self.path_in_repo(path):
            return True

        return self.repo.is_dirty(path=path)

    def get_head_commit(self):
        try:
            return self.repo.head.commit
        except (ValueError,) + ANY_GIT_ERROR:
            return None

    def get_head_commit_sha(self, short=False):
        commit = self.get_head_commit()
        if not commit:
            return
        if short:
            return commit.hexsha[:7]
        return commit.hexsha

    def get_head_commit_message(self, default=None):
        commit = self.get_head_commit()
        if not commit:
            return default
        return commit.message

    def get_current_branch(self):
        """Gets the name of the current active branch."""
        try:
            return self.repo.active_branch.name
        except (TypeError, AttributeError) + ANY_GIT_ERROR as e:
            # TypeError: detached HEAD state
            # AttributeError: If repo is None or active_branch is None
            # Handle detached HEAD state or other errors
            if "detached" in str(e).lower() or isinstance(e, TypeError):
                 # Try to get the commit hash instead for detached HEAD
                 try:
                     return self.repo.head.object.hexsha[:7] + " (Detached HEAD)"
                 except Exception:
                     raise ValueError("Could not determine current branch or commit (Detached HEAD).") from e
            raise ValueError(f"Could not determine current branch: {e}") from e

    def checkout_branch(self, branch_name):
        """Checks out the specified branch."""
        if self.is_dirty():
            raise ValueError("Working directory is dirty. Please commit or stash changes before switching branches.")
        try:
            self.repo.git.checkout(branch_name)
            self.io.tool_output(f"Switched to branch '{branch_name}'.")
        except git.exc.GitCommandError as e:
            raise ValueError(f"Could not checkout branch '{branch_name}': {e}") from e

    def checkout_primary_branch(self):
        """Checks out the primary (e.g., main) branch."""
        self.checkout_branch(self.primary_branch)

    def checkout_develop_branch(self):
        """Checks out the develop branch."""
        if not self.develop_branch:
            raise ValueError("Develop branch name is not set.")
        self.checkout_branch(self.develop_branch)

    def create_feature_branch(self, suffix=None):
        """Creates and checks out a new feature branch from the develop branch."""
        if self.is_dirty():
            raise ValueError("Working directory is dirty. Please commit or stash changes before creating a new branch.")

        # Ensure we are on the develop branch first
        current_branch = self.get_current_branch()
        if current_branch != self.develop_branch:
             self.io.tool_output(f"Switching to develop branch '{self.develop_branch}' to create feature branch...")
             try:
                 self.checkout_develop_branch()
             except ValueError as e:
                 raise ValueError(f"Could not switch to develop branch '{self.develop_branch}' to create feature branch: {e}") from e

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        # Use short hash of current develop branch HEAD for uniqueness
        try:
             short_hash = self.repo.commit(self.develop_branch).hexsha[:6]
        except Exception:
             short_hash = "nohash" # Fallback

        base_name = f"aider/feature-{timestamp}-{short_hash}"
        if suffix:
            # Sanitize suffix: replace spaces/invalid chars with hyphens
            safe_suffix = re.sub(r'[^\w\-]+', '-', suffix).strip('-').lower()
            branch_name = f"{base_name}-{safe_suffix}"
        else:
            branch_name = base_name

        # Truncate if too long (Git has limits, often around 255 chars, be conservative)
        max_len = 100
        if len(branch_name) > max_len:
            branch_name = branch_name[:max_len]

        try:
            # Check if branch already exists
            if branch_name in [b.name for b in self.repo.branches]:
                 self.io.tool_warning(f"Branch '{branch_name}' already exists. Checking it out.")
                 self.checkout_branch(branch_name)
                 return branch_name

            # Create and checkout the new branch
            new_branch = self.repo.create_head(branch_name)
            new_branch.checkout()
            self.io.tool_output(f"Created and switched to new branch '{branch_name}'.")
            return branch_name
        except git.exc.GitCommandError as e:
            raise ValueError(f"Could not create or checkout feature branch '{branch_name}': {e}") from e

    def list_aider_branches(self):
        """Lists branches matching the 'aider/*' pattern, plus the primary branch."""
        try:
            aider_branches = set(branch.name for branch in self.repo.branches if branch.name.startswith("aider/"))
            # Ensure the primary branch is included
            if self.primary_branch:
                aider_branches.add(self.primary_branch)
            return sorted(list(aider_branches))
        except Exception as e:
            raise ValueError(f"Could not list relevant branches: {e}") from e
