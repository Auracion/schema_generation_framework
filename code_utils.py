import os, sys, json
sys.path.append(os.path.dirname(__file__))
from copy import deepcopy


class PathCode:

    def __init__(self, path=None):
        self.path = [] if path is None else path

    def add(self, n):
        self.path.append(n)

    @property
    def string(self):
        return '-'.join(self.path)

    def copy(self, n=None):
        pc = PathCode(deepcopy(self.path))
        if n is not None:
            pc.add(n)
        return pc

    def __getitem__(self, item):
        return self.path[item]

    def __len__(self):
        return len(self.path)

    def __iter__(self):
        yield from self.path

    def __eq__(self, other):
        return '-'.join(self.path) == '-'.join(other.path)

    def insert(self, index, value):
        self.path.insert(index, value)

    def extend(self, ext):
        self.path.extend(ext)

    def __hash__(self):
        return '-'.join(self.path)

    def __repr__(self):
        return "PathCode: " + '-'.join(self.path)


class BranchCode:

    def __init__(self, branch=None):
        self.branch = [] if branch is None else branch

    def add(self, b):
        self.branch.append(b)

    @property
    def string(self):
        return ''.join(self.branch)

    @property
    def value(self):
        return int('0b' + self.string, 2)

    def copy(self, b=None):
        bc = BranchCode(deepcopy(self.branch))
        if b is not None:
            bc.add(b)
        return bc

    def __getitem__(self, item):
        return self.branch[item]

    def __len__(self):
        return len(self.branch)

    def __iter__(self):
        yield from self.branch

    def insert(self, index, value):
        self.branch.insert(index, value)

    def extend(self, ext):
        self.branch.extend(ext)

    def edit(self, index, value):
        self.branch[index] = value

    def __hash__(self):
        return '-'.join(self.branch)

    def __repr__(self):
        return "BranchCode: " + '-'.join(self.branch)


def reduce_from_branch(all_code, from_code):
    prefix = []
    from_path_code, from_branch_code = from_code
    for i in range(len(from_path_code) - 1):
        prefix.append(from_path_code[i] + from_branch_code[i])
    prefix.append("from")

    for (path, branch) in all_code:
        if path[-1] != 'from':
            continue
        tmp_path = []
        for i in range(len(path) - 1):
            tmp_path.append(path[i] + branch[i])
        tmp_path.append("from")
        if ('-'.join(tmp_path)) == ('-'.join(prefix)):
            if int(branch[-1]) > int(from_branch_code[-1]):
                branch.branch[-1] = str(int(branch[-1]) - 1)


def reduce_on_branch(all_code, on_code):
    prefix = []
    on_path_code, on_branch_code = on_code
    for i, node in enumerate(on_path_code):
        if node == "on":
            break
        else:
            prefix.append(node + on_branch_code[i])
    prefix.append("on")

    for (path, branch) in all_code:
        tmp_path = []
        for i, node in enumerate(path):
            if node == "on":
                break
            tmp_path.append(node + branch[i])
        tmp_path.append("on")
        if ('-'.join(tmp_path)) == ('-'.join(prefix)):
            if int(branch[-3]) > int(on_branch_code[-3]):
                branch.branch[-3] = str(int(branch[-3]) - 1)


def add_from_code(all_code, on_code):
    prefix = []
    path_code, branch_code = [], []
    on_path_code, on_branch_code = on_code
    for i, node in enumerate(on_path_code):
        if node == "on":
            break
        else:
            prefix.append(node + on_branch_code[i])
            path_code.append(node)
            branch_code.append(on_branch_code[i])
    prefix.append("from")
    max_idx = 0
    for (path, branch) in all_code:
        if path[-1] != 'from':
            continue
        tmp_path = []
        for i in range(len(path) - 1):
            tmp_path.append(path[i] + branch[i])
        tmp_path.append("from")
        if ('-'.join(tmp_path)) == ('-'.join(prefix)):
            if int(branch[-1]) >= max_idx:
                max_idx = int(branch[-1]) + 1
    path_code.append("from")
    branch_code.append(str(max_idx))
    return PathCode(path_code), BranchCode(branch_code)


def add_on_code(all_code, on_code):
    prefix = []
    path_code, branch_code = [], []
    on_path_code, on_branch_code = on_code
    for i, node in enumerate(on_path_code):
        if node == "on":
            break
        else:
            prefix.append(node + on_branch_code[i])
            path_code.append(node)
            branch_code.append(on_branch_code[i])
    prefix.append("on")
    max_idx = 0
    for (path, branch) in all_code:
        tmp_path = []
        for i, node in enumerate(path):
            if node == "on":
                break
            tmp_path.append(node + branch[i])
        tmp_path.append("on")
        if ('-'.join(tmp_path)) == ('-'.join(prefix)):
            if int(branch[-3]) >= max_idx:
                max_idx = int(branch[-3]) + 1
    path_code += ["on", "=", "Unary"]
    return PathCode(path_code), BranchCode(branch_code + [str(max_idx), "0", "0"]), BranchCode(branch_code + [str(max_idx), "1", "0"])


def move_on_to_where(all_code, left_code, right_code):
    prefix, prefix_wob = [], []
    left_path_code, left_branch_code = left_code
    right_path_code, right_branch_code = right_code
    for i, node in enumerate(left_path_code):
        if node == "on":
            break
        else:
            prefix.append(node + left_branch_code[i])
            prefix_wob.append(node)
    prefix.append("where0")
    prefix_wob.append("where")
    maxprefix, maxprefix_wob, maxdeep, maxvalue = find_max_prefix(all_code, prefix, prefix_wob)

    for (path, branch) in all_code:
        tmp_path = deepcopy(path.path)
        for i, node in enumerate(path):
            tmp_path[i] += branch[i]
        if '-'.join(tmp_path).startswith('-'.join(maxprefix)):
            prefix_windows = branch[len(left_path_code):len(left_path_code) + maxdeep]
            if len(prefix_windows) > 0 and prefix_windows[-1] == '2':
                prefix_windows[-1] = '1'
            if maxdeep == 0 or \
                    (len(branch) >= len(left_path_code) + maxdeep and
                     int('0b' + ''.join(prefix_windows), 2) >= maxvalue - 1):
                path.insert(len(maxprefix_wob) - 1, "and")
                branch.insert(len(maxprefix_wob) - 1, "0")

    if maxprefix_wob[-1] == "where":
        left_path_code.extend(['=', "Unary"])
        left_branch_code.branch[-1] = '0'
        left_branch_code.extend(['0', '0'])
        right_path_code.extend(['=', "Unary"])
        right_branch_code.branch[-1] = '0'
        right_branch_code.extend(['1', '0'])
    else:
        left_path_code.path = deepcopy(maxprefix_wob[:-1]) + ["and", "=", "Unary"]
        left_branch_code.branch = deepcopy(left_branch_code[:-1] + list('0' + bin(maxvalue)[2:-1] + "100"))
        right_path_code.path = deepcopy(maxprefix_wob[:-1]) + ["and", "=", "Unary"]
        right_branch_code.branch = deepcopy(right_branch_code[:-1] + list('0' + bin(maxvalue)[2:-1] + "110"))


def add_condition_to_where(all_code, from_code, value):
    from database_utils import Value
    prefix, prefix_wob = [], []
    from_path_code, from_branch_code = from_code
    cond_path_code, cond_branch_code = deepcopy(from_code)
    for i, node in enumerate(from_path_code):
        if node == "from" and i == len(from_path_code) - 1:
            cond_path_code.path[i] = "where"
            break
        else:
            prefix.append(node + from_branch_code[i])
            prefix_wob.append(node)
    prefix.append("where0")
    prefix_wob.append("where")
    maxprefix, maxprefix_wob, maxdeep, maxvalue = find_max_prefix(all_code, prefix, prefix_wob)

    for (path, branch) in all_code:
        tmp_path = deepcopy(path.path)
        for i, node in enumerate(path):
            tmp_path[i] += branch[i]
        # print('-'.join(tmp_path), '-'.join(maxprefix), branch.branch, maxdeep, maxvalue)
        if '-'.join(tmp_path).startswith('-'.join(maxprefix)):
            prefix_windows = branch[len(cond_path_code):len(cond_path_code) + maxdeep]
            if len(prefix_windows) > 0 and prefix_windows[-1] == '2':
                prefix_windows[-1] = '1'
            if maxdeep == 0 or \
                    (len(branch) >= len(cond_path_code) + maxdeep and
                     int('0b' + ''.join(prefix_windows), 2) >= maxvalue - 1):
                path.insert(len(maxprefix_wob) - 1, "and")
                branch.insert(len(maxprefix_wob) - 1, "0")
        # elif path == from_path_code:
        #     if int(branch[-1]) > int(cond_branch_code[-1]):
        #         branch.branch[-1] = str(int(branch[-1]) - 1)

    if maxprefix_wob[-1] == "where":
        cond_branch_code.branch[-1] = '0'
        value_path = cond_path_code.copy('=')
        value_branch = cond_branch_code.copy('1')
        cond_value = Value(value, value_path, value_branch)
        cond_path_code.extend(['=', "Unary"])
        cond_branch_code.extend(['0', '0'])
    else:
        value_path = PathCode(deepcopy(maxprefix_wob[:-1]) + ["and", "="])
        value_branch = BranchCode(deepcopy(cond_branch_code[:-1] + list('0' + bin(maxvalue)[2:-1] + "11")))
        cond_value = Value(value, value_path, value_branch)
        cond_path_code.path = deepcopy(maxprefix_wob[:-1]) + ["and", "=", "Unary"]
        cond_branch_code.branch = deepcopy(cond_branch_code[:-1] + list('0' + bin(maxvalue)[2:-1] + "100"))

    return cond_path_code, cond_branch_code, cond_value


def find_max_prefix(all_codes, prefix, prefix_wob):
    maxdeep = 0
    maxvalue = 0
    maxprefix = prefix
    maxprefix_wob = prefix_wob
    for (path_code, branch_code) in all_codes:
        tmp_path = deepcopy(path_code.path)
        for i, (node, branch) in enumerate(zip(path_code, branch_code)):
            tmp_path[i] += branch

        if '-'.join(tmp_path).startswith('-'.join(prefix)):
            where_clause_wob = path_code[len(prefix):]  # list of str
            where_clause = tmp_path[len(prefix):]
            i = 0
            for node in where_clause_wob:
                if node in ["unit", "union", "intersect", "except", "Unary", "Plus", "Minus", "Times", "Divide", "Count",
                          "Avg", "Sum", "Min", "Max", "or"]:
                    break
                i += 1

            if i == 1:  # where-cmp
                maxprefix_wob = prefix_wob + [where_clause_wob[0]]
                maxprefix = prefix + [where_clause[0]]
                maxdeep = i
                break
            elif i == 0:  # where-or
                maxprefix_wob = prefix_wob
                maxprefix = prefix
                maxdeep = i
                break
            prefix_windows = branch_code[len(prefix):len(prefix) + i]
            if prefix_windows[-1] == '2':
                prefix_windows[-1] = '1'
            branch_value = int('0b' + ''.join(prefix_windows), 2)
            if branch_value > maxvalue:
                maxvalue = branch_value
                maxdeep = i
                maxprefix_wob = prefix_wob + where_clause_wob[:i]
                maxprefix = prefix + where_clause[:i]

    maxprefix[-1] = maxprefix[-1][:-1]

    return maxprefix, maxprefix_wob, maxdeep, maxvalue
