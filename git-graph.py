#!/usr/bin/env python3

import os
import sys
import argparse
import re
import logging
import logging.config
import subprocess
import hashlib
import datetime
import time
import functools
import html
import tempfile
import platform
from typing import List, Dict, Any

MAX_NODE_WIDTH = 60
MAX_NODE_ROW_REF = 3

# colors for node
COLOR_BRANCH = "red"
COLOR_TAG = "green"
COLOR_NODE_MERGE = "cornsilk2"
COLOR_NODE_CHERRY_PICK = "burlywood1"
COLOR_NODE_REVERT = "azure4"
COLOR_NODE_STASH = "red"

_master_branch = {'master', 'Main', 'main'}

# colors for branches
_colors = [
    "skyblue",
    "yellow",
    "yellowgreen",
    "gold",
    "goldenrod",
    "violet",
    "tomato",
    "springgreen",
    "steelblue",
    "tan",
    "thistle",
    "turquoise",
    "peru",
    "pink",
    "plum",
    "powderblue",
    "purple",

    "antiquewhite",
    "aquamarine",
    "beige",
    "bisque",
    "blanchedalmond",
    "burlywood",
    "cadetblue",
    "chartreuse",
    "cornflowerblue",
    "cornsilk",
    "cyan",
    "darkgoldenrod",
    "darkgreen",
    "darkkhaki",
    "darkolivegreen",
    "darkorange",
    "darksalmon",
    "darkseagreen",
    "darkslategray",
    "darkslategrey",
    "darkturquoise",
    "deeppink",
    "deepskyblue",
    "dimgray",
    "dimgrey",
    "dodgerblue",
    "firebrick",
    "floralwhite",
    "forestgreen",
    "gainsboro",
    "ghostwhite",
    "gray",
    "grey",
    "green",
    "greenyellow",
    "honeydew",
    "hotpink",
    "indianred",
    "indigo",
    "ivory",
    "khaki",
    "lavender",
    "lavenderblush",
    "lawngreen",
    "lemonchiffon",
    "lightblue",
    "lightcoral",
    "lightcyan",
    "lightgoldenrodyellow",
    "lightgray",
    "lightgreen",
    "lightgrey",
    "lightpink",
    "lightsalmon",
    "lightseagreen",
    "lightskyblue",
    "lightslategray",
    "lightslategrey",
    "lightsteelblue",
    "lightyellow",
    "limegreen",
    "linen",
    "magenta",
    "maroon",
    "mediumaquamarine",
    "mediumblue",
    "mediumorchid",
    "mediumpurple",
    "mediumseagreen",
    "mediumslateblue",
    "mediumspringgreen",
    "mediumturquoise",
    "mediumvioletred",
    "midnightblue",
    "mintcream",
    "mistyrose",
    "moccasin",
    "navajowhite",
    "navy",
    "oldlace",
    "olive",
    "olivedrab",
    "orange",
    "orangered",
    "orchid",
    "palegoldenrod",
    "palegreen",
    "paleturquoise",
    "palevioletred",
    "papayawhip",
    "peachpuff",
    "red",
    "rosybrown",
    "royalblue",
    "saddlebrown",
    "salmon",
    "sandybrown",
    "seagreen",
    "seashell",
    "sienna",
    "slateblue",
    "slategray",
    "slategrey",
]


def get_color(index):
    return _colors[index % len(_colors)]


def get_logging_conf(verbose: bool = False, path: str = '/tmp'):
    logging_conf = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'standard': {
                'format': '[%(levelname)-5.5s] %(message)s'
            }
        },
        'filters': {},
        'handlers': {
            'console': {
                'level': 'DEBUG' if verbose else 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
                'filters': []
            },
            'log': {
                'level': 'DEBUG',
                'class': 'logging.FileHandler',
                'formatter': 'standard',
                'filename': '%s/git-graph.log' % (path),
                'encoding': 'utf-8',
                'mode': 'w'
            }
        },
        'loggers': {
            '': {
                'level': 'DEBUG',
                'handlers': ['console', 'log'],
                'propagate': False
            },
            'logfile': {
                'level': 'DEBUG',
                'handlers': ['log'],
                'propagate': False
            }
        }
    }
    return logging_conf


def break_line(line: str, indent: str = "", breaker: str = "\n"):
    out = ''
    pos = 0
    while pos < len(line):
        if pos != 0:
            out += breaker

        need = len(line) - pos
        if MAX_NODE_WIDTH - len(indent) < need:
            need = MAX_NODE_WIDTH - len(indent)
        out += indent
        out += html.escape(line[pos:pos + need])
        pos += need
    out += breaker
    return out


def branch_cmp(left, right):
    if (left in _master_branch) and (right in _master_branch):
        return 0
    elif left == right:
        return 0
    elif left in _master_branch and right not in _master_branch:
        return -1
    elif left not in _master_branch and right in _master_branch:
        return 1
    elif '/' not in left and '/' in right:
        return -1
    elif '/' in left and '/' not in right:
        return 1
    else:
        return -1 if left < right else 1


def elapse_func(info="cost"):
    def _elapse_func(fn):
        @functools.wraps(fn)
        def _wrapper(*args, **kwargs):
            start = time.clock_gettime(time.CLOCK_MONOTONIC)
            fn(*args, **kwargs)
            logging.debug('%s %s %s seconds' % (
                fn.__name__, info, time.clock_gettime(time.CLOCK_MONOTONIC) - start))
        return _wrapper
    return _elapse_func


class GitCommit:
    id: str = None
    time: int = None
    user: str = None
    message: str = None
    parents: List[str] = None
    fake_parents: List[str] = None
    ref: str = None

    head: str = None
    branch: str = None
    branches: List[str] = None
    tags: List[str] = None

    # Extended attributes
    stash: bool = False

    diff_hash: str = None
    cherry_pick_from: str = None
    revert: str = None

    def __init__(self, id, time, user, message, ref, parent, stash: bool = False):
        self.id = id
        self.time = int(time) if time else 0
        self.user = user
        self.message = message
        self.parents = parent.split(' ')
        self.fake_parents = []
        self.branches = []
        self.tags = []
        self.ref = ref if ref else ''
        if ref:
            self.__ref_parse(ref)
        self.stash = stash

    def __ref_parse(self, text: str):
        refs = text.replace("(", "").replace(")", "").split(",")
        for ref in refs:
            if ref.startswith('HEAD -> '):
                self.head = ref.replace("HEAD -> ", "").strip()
                self.branches.append(ref.replace("HEAD -> ", "").strip())
            elif 'tag' in ref:
                self.tags.append(ref.replace("tag: ", "").strip())
            elif 'stash' in ref:
                # nothing to do
                pass
            else:
                self.branches.append(ref.strip())
        self.branches = sorted(
            self.branches, key=functools.cmp_to_key(branch_cmp))

    def clone(self):
        commit = GitCommit(self.id, str(self.time), self.user,
                           self.message, self.ref, ' '.join(self.parents), self.stash)
        commit.head = self.head
        commit.branch = self.branch
        commit.diff_hash = self.diff_hash
        commit.cherry_pick_from = self.cherry_pick_from
        commit.revert = self.revert
        return commit

    def parent(self):
        return self.__parent(0)

    def parent2(self):
        return self.__parent(1)

    def parent3(self):
        return self.__parent(2)

    def __parent(self, index):
        if len(self.parents) > index:
            return self.parents[index]
        return None

    def set_fake_parent(self, id):
        if id not in self.fake_parents:
            self.fake_parents.append(id)

    def fake_parent(self):
        return self.__fake_parent(0)

    def fake_parent2(self):
        return self.__fake_parent(1)

    def __fake_parent(self, index):
        if len(self.fake_parents) > index:
            return self.fake_parents[index]
        return None

    def git_branch(self):
        if self.branch:
            return self.branch
        elif self.head:
            return self.head
        elif len(self.branches) > 0:
            return self.branches[0]

    def get_time(self):
        return datetime.datetime.fromtimestamp(self.time)

    def __lt__(self, other):
        return self.time < other.time

    def __str__(self):
        return 'GitCommit: %d/%s/%s: %s (%s) branches:%s tags:%s' % (
            self.time, self.id, self.user, self.message, '/'.join(
                self.parents),
            ','.join(self.branches), ','.join(self.tags))


class Git:
    logger = None
    pattern = re.compile(
        r'^\[(\d+)\|\|(.*)\|\|(.*)\|\|\s?(.*)\]\s([0-9a-f]*)\s?(.*)$')
    revert_pattern = re.compile(r'Revert "(.*)"')

    def __init__(self):
        self.logger = logging.getLogger("logfile")

    def __exec(self, cmd, exception=True, log=True):
        logging.debug('Run cmd: %s' % (cmd))
        proc = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, universal_newlines=True)
        (out, err) = proc.communicate()

        if log:
            self.logger.debug(
                '  result: %d, output: [\n%s]' % (proc.returncode, out))

        if not exception:
            return (proc.returncode, out)
        if proc.returncode != 0:
            raise RuntimeError('Run git command error: [%s]' % (cmd))
        return out

    def log(self, author_time: bool, author_name: bool, argv: List[str]):
        time = 'a' if author_time else 'c'
        name = 'a' if author_name else 'c'

        limit = ' '.join(argv)
        if not limit:
            limit = "-n 100"

        cmd = 'git log --pretty=format:"[%%%st||%%%sn||%%s||%%d] %%h %%p" %s' % (
            time, name, limit)
        return self.__exec(cmd).split('\n')

    def _log(self, argv: List[str]):
        cmd = 'git log %s' % (' '.join(argv))
        return self.__exec(cmd).split('\n')

    def stash(self, author_time: bool, author_name: bool, argv: List[str] = []):
        time = 'a' if author_time else 'c'
        name = 'a' if author_name else 'c'

        limit = ' '.join(argv)
        if not limit:
            limit = "-n 100"

        cmd = 'git stash list --pretty=format:"[%%%st||%%%sn||%%s||%%d] %%h %%p" %s' % (
            time, name, limit)
        return self.__exec(cmd).split('\n')

    def diff(self, hash):
        cmd = 'git diff %s^ %s | grep "^[-+]"' % (hash, hash)
        (result, out) = self.__exec(cmd, False, False)
        if result == 0:
            return out
        # Maybe binary file diff
        cmd = 'git diff %s^ %s' % (hash, hash)
        return self.__exec(cmd, True, False)

    def diff_hash(self, hash):
        diff = self.diff(hash)
        sha = hashlib.sha1(diff.encode('utf-8'))
        return sha.hexdigest()

    def is_parent_of(self, hash, start):
        # cmd = 'git log --reverse --ancestry-path --pretty=format:"%%h" %s^..%s | grep %s' % (
        #    hash, start, hash)
        cmd = 'git merge-base --is-ancestor %s %s' % (hash, start)
        (result, out) = self.__exec(cmd, False)
        return result == 0

    def ancestors_of(self, commits: List[str]):
        cmd = 'git merge-base --octopus %s' % (' '.join(commits))
        (result, out) = self.__exec(cmd, False)
        return out.strip()

    def fork_point(self, commits: List[str]):
        cmd = 'git merge-base --fork-point %s' % (' '.join(commits))
        (result, out) = self.__exec(cmd, False)
        return out.strip()

    '''
    This always return the closest branch name.
    '''

    def ___branch_of_(self, hash):
        name = self.name_rev(
            hash, ['''--exclude='tags/*' ''', '''--exclude='remotes/*' ''']).strip()
        if not name:
            name = self.name_rev(hash, ['''--exclude='tags/*' ''']).strip()
        pos = name.rfind('~')
        if pos != -1:
            return name[:pos]
        else:
            return name

    def branch_contains(self, hash):
        cmd = '''git branch -a --contains %s | sed 's/*//g' ''' % (hash)
        return self.__exec(cmd, False)[1].strip().split('\n')

    def name_rev(self, hash: str, argv: List[str] = []):
        cmd = '''git name-rev %s --name-only --no-undefined %s''' % (
            ' '.join(argv), hash)
        return self.__exec(cmd)


class GitBranch:
    name: str = None
    start: str = None
    last: str = None
    parent: str = None
    commits: List[str] = None

    def __init__(self, name, start: str, parent: str = None):
        self.name = name
        self.start = start
        self.last = start
        self.parent = parent
        self.commits = []

    def add_commit(self, commit: GitCommit):
        self.commits.append(commit.id)
        if self.parent == commit.id:
            self.last = commit.id
            self.parent = commit.parent()
        if commit.branch is None:
            commit.branch = self.name
        elif commit.branch != self.name:
            logging.warn("Commit %s's branch is %s not %s" %
                         (commit.id, commit.branch, self.name))

    def add_commit_force(self, commit: GitCommit):
        self.commits.append(commit.id)
        self.last = commit.id
        self.parent = commit.parent()
        commit.branch = self.name

    def _is_branch_commit(self, commit: GitCommit):
        if commit.id == self.parent:
            return True
        if commit.parent() and commit.parent() in self.commits:
            return True
        return False

    def __eq__(self, other):
        return branch_cmp(self.name, other.name) == 0

    def __lt__(self, other):
        return branch_cmp(self.name, other.name) < 0

    def __str__(self):
        return self.name


class GitGraph:
    git: Git = None
    options: Dict[str, Any] = None

    commits: List[GitCommit] = None
    commits_id: Dict[str, GitCommit] = None
    commits_subject: Dict[str, List[GitCommit]] = None

    branches: List[GitBranch] = None
    branches_id: Dict[str, GitBranch] = None
    current_branch: str = None

    def __init__(self, author_time: bool = True, author_name: bool = True,
                 branch: List[str] = [], argv: List[str] = [],
                 cherry_pick: bool = True, revert: bool = True):
        self.git = Git()
        self.options = {
            'author_time': author_time,
            'author_name': author_name,
            'branch': branch,
            'argv': argv,
            'cherry_pick': cherry_pick,
            'revert': revert
        }

        self.commits = []
        self.commits_id = {}
        self.commits_subject = {}
        self.commits_detail = []
        self.branches = []
        self.branches_id = {}

    @elapse_func()
    def _parse_log(self):
        for line in self.git.log(self.options['author_time'], self.options['author_name'], self.options['argv']):
            match = re.match(self.git.pattern, line)
            if not match:
                logging.warn('Invalid commit: %s' % (line))
                continue
            date = match.group(1)
            user = match.group(2)
            message = match.group(3)
            ref = match.group(4)
            commit_id = match.group(5)
            parent_id = match.group(6)

            commit = GitCommit(commit_id, date, user, message, ref, parent_id)
            self.commits.append(commit)
            self.commits_id[commit_id] = commit
            if message not in self.commits_subject:
                self.commits_subject[message] = []
            self.commits_subject[message].append(commit)

    @elapse_func()
    def _parse_stash(self):
        for line in self.git.stash(self.options['author_time'], self.options['author_name']):
            match = re.match(self.git.pattern, line)
            if not match:
                logging.warn('Invalid commit: %s' % (line))
                continue
            date = match.group(1)
            user = match.group(2)
            message = match.group(3)
            commit_id = match.group(5)
            parent_id = match.group(6)

            commit = GitCommit(commit_id, date, user, message,
                               None, parent_id, True)
            self.commits.append(commit)
            self.commits_id[commit_id] = commit

    def _add_branch_by_name(self, name: str, commit: GitCommit):
        branch = GitBranch(name, commit.id, commit.parent())
        self.branches.append(branch)
        self.branches_id[name] = branch
        self.branches.sort()
        return branch

    def _add_branch(self, commit: GitCommit):
        name = commit.git_branch()
        if name is None:
            return None
        return self._add_branch_by_name(name, commit)

    def _find_branch_by_name(self, name: str):
        for br in self.branches:
            if br.name == name:
                return br
        return None

    def _find_branch(self, commit: GitCommit):
        for br in self.branches:
            if commit.id == br.parent:
                return br
        return None

    def _find_or_add_branch(self, commit: GitCommit):
        br = self._find_branch(commit)
        if br is None:
            br = self._add_branch(commit)
        if br:
            br.add_commit(commit)
        return br

    def _find_parent_branch(self, commit: GitCommit):
        current = commit
        while current:
            if current.branch:
                return self.branches_id[current.branch]
            if current.parent() and current.parent() in self.commits_id:
                current = self.commits_id[current.parent()]
            elif current.parent2() and current.parent2() in self.commits_id[current.parent2()]:
                current = self.commits_id[current.parent2()]
            else:
                break
        return None

    def _branch_of(self, hash):
        branches = self.git.branch_contains(hash)
        if len(branches) == 1:
            return branches[0]

        def branch_filter(branch):
            return branch in self.options['branch']
        if len(self.options['branch']) > 0:
            branches = list(filter(branch_filter, branches))

        if len(branches) == 0:
            return None
        elif len(branches) == 1:
            return branches[0]
        return self._branch_of_(hash)

    def _branch_of_(self, hash):
        argv = [
            '--ancestry-path',
            '--all',
            '%s^..' % (hash)]

        graph = GitGraph(self.options['author_time'], self.options['author_name'],
                         self.options['branch'], argv, False, False)
        graph.process()
        return graph.commits_id[hash].branch

    def _branch_fork_point(self, branch: GitBranch):
        if branch.parent in self.commits_id:
            parent = self.commits_id[branch.parent]
            if parent.branch != branch.name:
                return parent
        return None

    def __diff_hash(self, commit: GitCommit):
        if commit.diff_hash is None:
            commit.diff_hash = self.git.diff_hash(commit.id)
        return commit.diff_hash

    def _process_cherry_pick(self, commit: GitCommit):
        if commit.message not in self.commits_subject:
            return

        # Merge commit
        if len(commit.parents) > 1:
            return
        commit_same = self.commits_subject[commit.message]
        if len(commit_same) <= 1:
            return
        commit_same.sort()

        for c in commit_same:
            if c.id == commit.id or c.time > commit.time:
                continue

            if self.__diff_hash(commit) == self.__diff_hash(c):
                commit.cherry_pick_from = c.id
                break

    def _process_revert(self, commit: GitCommit):
        if not commit.message.startswith('Revert'):
            return
        match = re.match(self.git.revert_pattern, commit.message)
        if match:
            origin = match.group(1)
            if origin in self.commits_subject:
                for c in self.commits_subject[origin]:
                    if c.branch == commit.branch:
                        commit.revert = c.id
                        return
            logging.warn(
                'Revert commit origin commit not found: [%s/%s]' % (commit.id, commit.message))
        else:
            logging.warn(
                'Revert commit: format error: [%s/%s] ' % (commit.id, commit.message))

    def _process_detached_branch(self, commit: GitCommit):
        logging.debug('Process detached commit: %s' % (commit.id))
        name = self._branch_of(commit.id)
        if not name:
            logging.warn('''Can't detect the branch of commit %s''' %
                         (commit.id))
            return
        branch = self._find_branch_by_name(name)
        if branch:
            if branch._is_branch_commit(commit):
                branch.add_commit(commit)
            elif self.git.is_parent_of(commit.id, branch.last):
                self.commits_id[branch.last].set_fake_parent(commit.id)
                branch.add_commit_force(commit)
            else:
                logging.warn('''Can't detect position of commit %s in branch %s''' % (
                    commit.id, branch.name))
        else:
            branch = self._add_branch_by_name(name, commit)
            branch.add_commit(commit)

    @elapse_func()
    def _process_commits(self):
        commit_without_branch: List[str] = []
        commit_ignore: List[str] = []

        for commit in self.commits:
            if 'refs/stash' in commit.ref and not commit.stash:
                ignores = [commit.id, commit.parent2()]
                if commit.parent3():
                    ignores.append(commit.parent3())
                logging.debug('Ignore commit %s' % (' '.join(ignores)))
                commit_ignore += ignores

        # filter the stash which parent not exist
        def filter_commit(commit: GitCommit):
            if commit.stash and commit.parent() not in self.commits_id:
                return False
            elif commit.id in commit_ignore:
                return False
            return True
        self.commits = list(filter(filter_commit, self.commits))

        for commit in self.commits:
            if commit.head:
                self.current_branch = commit.head

            branch = self._find_or_add_branch(commit)
            if branch is None:
                commit_without_branch.append(commit.id)

            if self.options['cherry_pick']:
                self._process_cherry_pick(commit)

        for id in commit_without_branch:
            commit = self.commits_id[id]
            branch = self._find_parent_branch(commit)
            if branch:
                branch.add_commit(commit)
                continue

            self._process_detached_branch(commit)

        if self.options['revert']:
            for commit in self.commits:
                self._process_revert(commit)

    def process(self):
        self._parse_log()
        self._process_commits()


class GitGraphEx(GitGraph):
    graph_cache: GitGraph = None

    def __init__(self, enable_graph_cache: bool = True,
                 author_time: bool = True, author_name: bool = True,
                 branch: List[str] = [], argv: List[str] = []):
        GitGraph.__init__(self, author_time, author_name, branch, argv)

        if enable_graph_cache:
            argv = ['--all']
            self.graph_cache = GitGraph(True, True, branch, argv, False, False)

    def process(self):
        if self.graph_cache is not None:
            self.graph_cache.process()

        self._parse_log()
        # self._parse_stash()
        self._process_commits()

    def _process_commits(self):
        GitGraph._process_commits(self)
        if self.graph_cache is not None:
            self._process_branch_fork_point()
            self._process_branch_merge_point()

    def _process_branch_fork_point(self):
        for branch in self.branches:
            if branch.parent in self.commits_id:
                continue

            last = self.graph_cache.commits_id[branch.last]
            branch_cache = self.graph_cache._find_branch_by_name(last.branch)
            if not branch_cache:
                continue

            fork_point = self.graph_cache._branch_fork_point(branch_cache)
            if not fork_point:
                continue

            point = fork_point
            while point:
                if point.id in self.commits_id:
                    break
                elif point.parent() and point.parent() in self.graph_cache.commits_id:
                    point = self.graph_cache.commits_id[point.parent()]
                else:
                    point = None

            if not point:
                point = fork_point.clone()
                self.commits.append(point)
                self.commits_id[point.id] = point
                self._process_detached_branch(point)

            last = self.commits_id[branch.last]
            last.set_fake_parent(point.id)

    def _process_branch_merge_point(self):
        for branch in self.branches:
            # TODO:
            pass

    def _branch_of_(self, hash):
        if self.graph_cache is None:
            return GitGraph._branch_of_(self, hash)

        if hash in self.graph_cache.commits_id:
            return self.graph_cache.commits_id[hash].branch
        return None


class GitGraphPrinter:
    graph: GitGraph = None
    options: Dict[str, Any] = None

    commits_detail: List[str] = None

    def __init__(self, graph, branch: List[str], type: str, output: str,
                 strict: bool = False, dot_args: str = ""):
        self.graph = graph
        self.options = {
            'branch': branch,
            'type': type,
            'output': output,
            'strict': strict,
            'dot_args': dot_args
        }
        self.commits_detail = []

    @elapse_func()
    def output_dot(self, path: str):
        fp = None
        if not path:
            fp = sys.stdout
        else:
            fp = open(path, "w", encoding='utf-8')
        self._output_dot(fp)

        if fp != sys.stdout:
            fp.close()

    def _output_dot(self, fp):
        self._output_dot_head(fp)
        self._output_dot_title(fp)
        self._output_dot_branches(fp)
        self._output_dot_edge(fp)
        self._output_dot_tail(fp)

    def _output_dot_head(self, fp):
        # head
        if self.options['strict']:
            fp.write('strict ')

        fp.write('digraph G {\n')
        fp.write('  rankdir=BT;\n')
        fp.write('  splines="line";\n')
        fp.write(
            '  graph [compound=false,sep=1,nodesep="0.3",ranksep="0.3"];\n')
        fp.write('  node [shape=box];\n')
        fp.write('\n')

    def _output_dot_title(self, fp):
        # title
        title = 'Current branch: %s\\n' % (self.graph.current_branch)
        fp.write('  labelloc="t";\n')
        fp.write('  fontsize=36;\n')
        fp.write('  label="%s";\n' % (title))
        fp.write('\n')

    def _output_dot_branches(self, fp):
        # branches
        for idx in range(len(self.graph.branches)):
            branch = self.graph.branches[idx]
            if len(branch.commits) == 0:
                continue

            if self.options['branch'] and not branch.name in self.options['branch']:
                logging.debug('Exclude branch %s' % (branch.name))
                continue

            fp.write('  subgraph "cluster_%s" {\n' % (branch.name))
            fp.write('    style=filled;\n')
            fp.write('    color=%s;\n' % (get_color(idx)))
            fp.write('    label="%s";\n' % (branch.name))
            fp.write('    labelloc="b";\n')
            fp.write('    fontsize=36;\n')

            # commits
            for id in branch.commits:
                commit = self.graph.commits_id[id]
                self._output_dot_commit(fp, commit)

            fp.write('  }\n')

    def _output_dot_commit(self, fp, commit: GitCommit, indent: str = "    "):
        label = '<'

        label += '<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="1">'

        # reference names(branch and tags)
        label += self._format_commit_refs(commit)

        # commit id, user, date and message
        label += '<TR>'
        label += '<TD ALIGN="LEFT" COLSPAN="%d">%s</TD>' % (1, commit.id)
        label += '<TD ALIGN="LEFT" COLSPAN="%d">%s</TD>' % (1, commit.user)
        label += '<TD ALIGN="RIGHT" COLSPAN="%d">%s</TD>' % (
            MAX_NODE_ROW_REF - 2, commit.get_time().strftime('%Y-%m-%d %H:%M:%S'))
        label += '</TR>'

        # message
        label += '<TR>'
        label += '<TD ALIGN="LEFT" COLSPAN="%d">%s</TD>' % (
            MAX_NODE_ROW_REF, break_line(commit.message, "", '<BR ALIGN="LEFT"/>'))
        label += '</TR>'

        label += '</TABLE>'
        label += '>'

        color = None
        if commit.stash:
            color = COLOR_NODE_STASH
        elif commit.revert:
            color = COLOR_NODE_REVERT
        elif commit.parent2():
            color = COLOR_NODE_MERGE

        if color:
            fp.write('%s"%s" [label=%s, fontsize=%d, fillcolor="%s"];\n' % (
                indent, commit.id, label, 16, color))
        else:
            fp.write('%s"%s" [label=%s, fontsize=%d];\n' %
                     (indent, commit.id, label, 16))
        self.commits_detail.append(commit.id)

    def _format_commit_refs(self, commit: GitCommit):
        if len(commit.branches) == 0 and len(commit.tags) == 0:
            return ''

        out = '<TR>'

        format = '<TD ALIGN="LEFT" BGCOLOR="%s" BORDER="1">%s</TD>'
        row_cnt = 0

        for branch in commit.branches:
            if row_cnt >= MAX_NODE_ROW_REF:
                out += '</TR>'
                out += '<TR>'
                row_cnt = 0
            out += format % (COLOR_BRANCH, branch) + " "
            row_cnt += 1

        for tag in commit.tags:
            if row_cnt >= MAX_NODE_ROW_REF:
                out += '</TR>'
                out += '<TR>'
                row_cnt = 0
            out += format % (COLOR_TAG, tag) + " "
            row_cnt += 1

        out += '</TR>'
        return out

    def _output_dot_edge_parent(self, fp, commit: GitCommit, parent):
        if parent not in self.graph.commits_id:
            return
        if parent not in self.commits_detail:
            self._output_dot_commit(fp, self.graph.commits_id[parent], "  ")

        fp.write('  "%s" -> "%s"\n' % (parent, commit.id))

    def _output_dot_edge_fake_parent(self, fp, commit: GitCommit, parent):
        if parent not in self.graph.commits_id:
            return
        if parent not in self.commits_detail:
            self._output_dot_commit(fp, self.graph.commits_id[parent], "  ")

        fp.write('  "%s" -> "%s" [style=dashed]\n' % (parent, commit.id))

    def _output_dot_edge_cherry_pick(self, fp, commit: GitCommit):
        if not commit.cherry_pick_from:
            return
        if commit.cherry_pick_from not in self.commits_detail:
            self._output_dot_commit(
                fp, self.graph.commits_id[commit.cherry_pick_from], "  ")
        fp.write('  "%s" -> "%s" [label="Cherry pick",fontcolor=red,color=red]\n' % (
            commit.cherry_pick_from, commit.id))

    def _output_dot_edge_revert(self, fp, commit: GitCommit):
        if not commit.revert:
            return
        if commit.revert not in self.commits_detail:
            self._output_dot_commit(
                fp, self.graph.commits_id[commit.revert], "  ")
        fp.write('  "%s" -> "%s" [label="Revert",fontcolor=azure4,color=azure4]\n' % (
            commit.id, commit.revert))

    def _output_dot_edge(self, fp):
        for commit in self.graph.commits:
            if self.options['branch'] and commit.branch not in self.options['branch']:
                continue
            if commit.id not in self.commits_detail:
                self._output_dot_commit(fp, commit, "  ")

            if commit.parent():
                self._output_dot_edge_parent(fp, commit, commit.parent())
            if commit.parent2():
                self._output_dot_edge_parent(fp, commit, commit.parent2())

            if commit.fake_parent():
                self._output_dot_edge_fake_parent(
                    fp, commit, commit.fake_parent())
            if commit.fake_parent2():
                self._output_dot_edge_fake_parent(
                    fp, commit, commit.fake_parent2())

            self._output_dot_edge_cherry_pick(fp, commit)
            self._output_dot_edge_revert(fp, commit)

    def _output_dot_tail(self, fp):
        fp.write("}\n")

    @elapse_func()
    def output_graph(self, path: str, type: str):
        # generate dot file
        fd, filename = tempfile.mkstemp(prefix='git-graph-', suffix='.dot')
        logging.info('Generating dot file: %s' % filename)

        fp = open(filename, "w", encoding='utf-8')
        self._output_dot(fp)
        fp.close()

        if path:
            output = path
        else:
            fd, output = tempfile.mkstemp(
                prefix='git-graph-', suffix=".%s" % (type))
            logging.info('Generating output file: %s' % output)

        self._output_graph(type, filename, output)

        if not path:
            if platform.system() == "Windows":
                os.system('start %s &' % (output))
            elif platform.system() == "Linux":
                os.system('xdg-open %s &' % (output))
            elif platform.system() == "Darwin":
                os.system('open %s &' % (output))
            else:
                pass

    def _output_graph(self, type: str, dotfile: str, output: str):
        cmd = 'dot -T %s -o "%s" %s %s' % (type,
                                           output, self.options['dot_args'], dotfile)
        ret = os.system(cmd)
        if ret != 0:
            sys.exit(ret)

    def output(self):
        if self.options['type'] == "dot":
            return self.output_dot(self.options['output'])
        else:
            return self.output_graph(self.options['output'], self.options['type'])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-V", "--verbose",
                        action='store_true', help="Show verbose log")

    parser.add_argument("--author_time", dest="author_time",
                        action="store_true", help="Use author time")
    parser.add_argument("--author_name", dest="author_name",
                        action="store_true", help="Use author name")
    parser.add_argument("--pretty", dest="",
                        help='Just ignore the --pretty argument')

    parser.add_argument("--branch", dest="branch", action='append',
                        help='Branch to display.')

    parser.add_argument("--strict", action="store_true", help="Strict graph")
    parser.add_argument("-o", "--output", help='Output file.', default="")
    parser.add_argument("-t", "--type", help="Output type.", default="svg")
    parser.add_argument("--dot-args", dest="dot_args",
                        help="Dot extra arguments", default="")

    (args, argv) = parser.parse_known_args()

    logging.config.dictConfig(get_logging_conf(args.verbose))

    graph = GitGraphEx(author_time=args.author_time,
                       author_name=args.author_name,
                       branch=args.branch if args.branch else [],
                       argv=argv)
    graph.process()

    printer = GitGraphPrinter(graph,
                              branch=args.branch if args.branch else [],
                              strict=args.strict,
                              output=args.output,
                              type=args.type,
                              dot_args=args.dot_args)
    printer.output()
