import os, sys, json, re, pickle
from collections import defaultdict
import nltk


def get_error_id():
    log_dir = 'data/log.txt'
    dev = json.load(open('data/dev.json', 'r'))
    idx = 0
    error_ids = []
    with open(log_dir, 'r') as f:
        for line in f:
            line = line.strip('\n')
            if line.startswith('easy gold:') or \
                    line.startswith('medium gold: ') or \
                    line.startswith('hard gold: ') or \
                    line.startswith('extra gold: '):
                query = ': '.join(line.split(': ')[1:])
                flag = False
                while idx < len(dev):
                    if dev[idx]['query'] != query:
                        idx += 1
                    else:
                        idx += 1
                        flag = True
                        break
                if flag:
                    error_ids.append(idx)
                else:
                    print(query)
                    break
    return error_ids


def get_global_sketch(sql):
    sketch = ''
    sketch += 'SELECT [Column] '
    flag = True
    for table in sql['from']['table_units']:
        if table[0] != "table_unit":
            flag = False
            break
    if flag:
        sketch += 'FROM [Table] '
    else:
        sketch += 'FROM [SQL] '

    if len(sql["where"]) > 0:
        sketch += 'WHERE '
        for cond in sql["where"]:
            if cond == 'or' or cond == 'and':
                sketch += 'AND '
            else:
                if isinstance(cond[3], dict):
                    sketch += 'SQL_Condition(' + get_global_sketch(cond[3]) + ') '
                elif isinstance(cond[3], list):
                    sketch += 'Column_Condition '
                else:
                    sketch += 'Value_Condition '

    if len(sql["groupBy"]) > 0:
        sketch += 'GROUPBY [Column] '

    if len(sql["having"]) > 0:
        sketch += 'HAVING '
        for cond in sql["having"]:
            if cond == 'or' or cond == 'and':
                sketch += 'AND '
            else:
                if isinstance(cond[3], dict):
                    sketch += 'SQL_Condition(' + get_global_sketch(cond[3]) + ') '
                elif isinstance(cond[3], list):
                    sketch += 'Column_Condition '
                else:
                    sketch += 'Value_Condition '

    if len(sql["orderBy"]) > 0:
        sketch += "ORDERBY [Column] "

    if sql["limit"] is not None:
        sketch += 'LIMIT [VALUE]'

    sketch = sketch.strip(' ')

    if sql['union'] is not None:
        sketch = sketch + ' UNION ' + get_global_sketch(sql['union'])

    if sql['intersect'] is not None:
        sketch = sketch + ' INTERSECT ' + get_global_sketch(sql['intersect'])

    if sql['except'] is not None:
        sketch = sketch + ' EXCEPT ' + get_global_sketch(sql['except'])

    return sketch


def statistic_global_sketch(dataset, annotations):
    all_sketch = defaultdict(list)
    for i, ant in enumerate(annotations):
        data = dataset[i]
        sql = data['sql']
        question = data['question_toks']
        query = data['query']
        sketch = get_global_sketch(sql)
        all_sketch[sketch].append((ant, question, query))

    order = sorted(all_sketch, key=lambda x: len(all_sketch[x]), reverse=True)
    ordered_sketch = {k: all_sketch[k] for k in order}

    return ordered_sketch


def get_global_template(sent_toks, ant):
    if len(sent_toks) != len(ant):
        return None
    tmp_toks = []
    for i, tok in enumerate(sent_toks):
        if ant[i] is None:
            tmp_toks.append(tok)
        else:
            labels = ant[i]
            tok_type = labels['type']
            labels['scope'] = labels.get('scope', 'main')
            if tok_type == 'tbl':
                # chunck = '[T]' + labels['scope']
                chunck = f'[T:{tok}]'
            elif tok_type == 'col':
                # chunck = '[C]' + labels['scope']
                # if 'func' in labels and len(labels['func']) > 0:
                #     chunck += '_' + labels['func'][0]
                chunck = f'[C:{tok}]'
            elif tok_type == 'val':
                # chunck = '[V]' + labels['scope']
                chunck = f'[V:{tok}]'
            else:
                raise ValueError
            tmp_toks.append(chunck)

    return tmp_toks


def statistic_global_template_by_slant(dataset, annotations):
    ordered_sketch = statistic_global_sketch(dataset, annotations)

    for sketch in ordered_sketch:
        if 'WHERE' in sketch:
            continue
        template_dict = {}
        for ant, question, query in ordered_sketch[sketch]:
            tmp_toks = get_global_template(question, ant)
            if tmp_toks is None:
                continue
            tmp_toks = ' '.join(tmp_toks)
            template_dict[tmp_toks] = template_dict.get(tmp_toks, 0) + 1
        print(sketch, len(ordered_sketch[sketch]), len(template_dict))
        order = sorted(template_dict, key=lambda x: template_dict[x], reverse=True)
        for tmp in order:
            print(tmp, template_dict[tmp])
        break


def get_template_by_freq(questions, freq, threshold):
    template_dist = {}
    for question in questions:
        template = []
        for tok in question:
            tok = tok.lower()
            if tok in ['the', 'distinct', 'different', 'how', 'many', 'do', 'we', 'have']:
                template.append(tok)
            else:
                if len(template) > 0 and template[-1] != '[T]':
                    template.append('[T]')
                # template.append(tok)
        template = ' '.join(template)
        template_dist[template] = template_dist.get(template, 0) + 1

    order = sorted(template_dist, key=lambda x: template_dist[x], reverse=True)
    dist = {k: template_dist[k] for k in order}
    return dist


def statistic_global_template_by_freq(dataset, annotations, rate):
    ordered_sketch = statistic_global_sketch(dataset, annotations)

    for sketch in ordered_sketch:
        # if 'WHERE' in sketch:
        #     continue
        freq = {}
        all_questions = []
        for ant, question, query in ordered_sketch[sketch]:
            all_questions.append(question)
            for tok in question:
                freq[tok.lower()] = freq.get(tok.lower(), 0) + 1
        template_dist = get_template_by_freq(all_questions, freq, threshold=rate * len(all_questions))
        print(sketch)
        print(f'Totally, there are {len(all_questions)}, distilled {len(template_dist)} template')
        print()
        for tmp in template_dist:
            print(tmp, template_dist[tmp])
        break


def get_verb_phrases(t):
    verb_phrases = []
    num_children = len(t)
    num_VP = sum(1 if t[i].label() == "VP" else 0 for i in range(0, num_children))

    if t.label() != "VP":
        for i in range(0, num_children):
            if t[i].height() > 2:
                verb_phrases.extend(get_verb_phrases(t[i]))
    elif t.label() == "VP" and num_VP > 1:
        for i in range(0, num_children):
            if t[i].label() == "VP":
                if t[i].height() > 2:
                    verb_phrases.extend(get_verb_phrases(t[i]))
    else:
        verb_phrases.append(' '.join(t.leaves()))

    return verb_phrases


def get_pos(t):
    vp_pos = []
    sub_conj_pos = []
    num_children = len(t)
    children = [t[i].label() for i in range(0, num_children)]

    flag = re.search(r"(S|SBAR|SBARQ|SINV|SQ)", ' '.join(children))

    if "VP" in children and not flag:
        for i in range(0, num_children):
            if t[i].label() == "VP":
                vp_pos.append(t[i].treeposition())
    elif not "VP" in children and not flag:
        for i in range(0, num_children):
            if t[i].height() > 2:
                temp1, temp2 = get_pos(t[i])
                vp_pos.extend(temp1)
                sub_conj_pos.extend(temp2)
    # comment this "else" part, if want to include subordinating conjunctions
    else:
        for i in range(0, num_children):
            if t[i].label() in ["S", "SBAR", "SBARQ", "SINV", "SQ"]:
                temp1, temp2 = get_pos(t[i])
                vp_pos.extend(temp1)
                sub_conj_pos.extend(temp2)
            else:
                sub_conj_pos.append(t[i].treeposition())

    return (vp_pos, sub_conj_pos)


def print_clauses(parse_str):
    sent_tree = nltk.tree.ParentedTree.fromstring(parse_str)
    clause_level_list = ["S", "SBAR", "SBARQ", "SINV", "SQ"]
    clause_list = []
    sub_trees = []
    # sent_tree.pretty_print()

    # break the tree into subtrees of clauses using
    # clause levels "S","SBAR","SBARQ","SINV","SQ"
    for sub_tree in reversed(list(sent_tree.subtrees())):
        if sub_tree.label() in clause_level_list:
            if sub_tree.parent().label() in clause_level_list:
                continue

            if (len(sub_tree) == 1 and sub_tree.label() == "S" and sub_tree[0].label() == "VP"
                    and not sub_tree.parent().label() in clause_level_list):
                continue

            sub_trees.append(sub_tree)
            del sent_tree[sub_tree.treeposition()]

    # for each clause level subtree, extract relevant simple sentence
    for t in sub_trees:
        # get verb phrases from the new modified tree
        verb_phrases = get_verb_phrases(t)

        # get tree without verb phrases (mainly subject)
        # remove subordinating conjunctions
        vp_pos, sub_conj_pos = get_pos(t)
        for i in reversed(vp_pos):
            del t[i]
        for i in reversed(sub_conj_pos):
            del t[i]

        subject_phrase = ' '.join(t.leaves())

        # update the clause_list
        for i in verb_phrases:
            clause_list.append(subject_phrase + " " + i)

    print(clause_list)
    return clause_list


def split_subclause(sentence, client):
    output = client.annotate(sentence)
    parse_tree = output['sentences'][0]['parse']
    parse_tree = ' '.join(parse_tree.split())

    print_clauses(parse_str=parse_tree)


def ngram_lexical(questions, n):
    ngram = {}
    for question in questions:
        for i in range(0, len(question) - n + 1):
            tokens = ' '.join(question[i:i + n])
            ngram[tokens] = ngram.get(tokens, 0) + 1
    return ngram


def check_schema_linking():
    dataset = pickle.load(open('gendata/hardness_affected_dev_1.0/dev.bin', 'rb'))
    # print(dataset[0]['db_id'])
    # print(dataset[0]['question'])
    # print(dataset[0]['query'])
    # print(dataset[0]['schema_linking'][0][2])
    count = 0
    for data in dataset:
        if 'c2a' not in data['db_id']:
            continue
        sl = data['schema_linking'][0]
        for ri, line in enumerate(sl):
            for ci, r in enumerate(line):
                if 'value' in r:
                    print(data['db_id'])
                    print(data['question'])
                    print(ri, ci)
                    print()
                    count += 1
    print(count)


def statistic_schema_linking(method=None):
    results = {'table': 0, 'column': 0, 'value': 0, 'none': 0, 'match': 0}
    if method is None:
        dataset = pickle.load(open(f'../text2sql-lgesql/data/dev.bin', 'rb'))
    elif method == 'ets':
        dataset = pickle.load(open(f'gendata/ets_affected_dev/dev.bin', 'rb'))
    else:
        dataset = pickle.load(open(f'gendata/methods/{method}/dev.bin', 'rb'))
    total = 0
    print(list(dataset[0].keys()))
    for data in dataset:
        total += 1
        sl = data['schema_linking'][1]
        for row in sl:
            for item in row:
                if 'nomatch' in item or '*' in item:
                    results['none'] += 1
                else:
                    if 'table' in item:
                        results['table'] += 1
                    elif 'value' in item:
                        results['value'] += 1
                    elif 'column' in item:
                        results['column'] += 1
                    results['match'] += 1
                    break
    print(method, total)
    for k in results:
        print(k, results[k] / total)
    print()


def statistic_original_schema_linking(method=None):
    results = {'table': 0, 'column': 0, 'value': 0, 'none': 0, 'match': 0}

    ori_dataset = pickle.load(open(f'../text2sql-lgesql/data/dev.bin', 'rb'))
    qes_key_dataset = {data['question']: data for data in ori_dataset}
    dataset = pickle.load(open(f'gendata/methods/{method}/dev.bin', 'rb'))
    total = 0
    for data in dataset:
        total += 1
        ori_data = qes_key_dataset[data['question']]
        sl = ori_data['schema_linking'][1]
        for row in sl:
            for item in row:
                if 'nomatch' in item or '*' in item:
                    results['none'] += 1
                else:
                    if 'table' in item:
                        results['table'] += 1
                    elif 'value' in item:
                        results['value'] += 1
                    elif 'column' in item:
                        results['column'] += 1
                    results['match'] += 1
                    break
    print(method, total)
    for k in results:
        print(k, results[k] / total)
    print()




if __name__ == '__main__':
    statistic_template()