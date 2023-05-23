import os, sys, json
import random

sys.path.append(os.path.dirname(__file__))

from aug_system import Generator
from eval.spider.evaluation import Evaluator

random.seed(42)


def statistic_hardness(mode, use_gen, affected):
    assert mode in ['dev', 'train']
    if mode == 'dev':
        dataset = json.load(open('data/dev.json', 'r'))
    else:
        dataset = json.load(open('data/train.json', 'r'))

    if use_gen:
        dbs = json.load(open('data/tables_with_tags.json', 'r'))
        dbs = {db["db_id"]: db for db in dbs}
        generator = Generator(n_step=1, affected=affected, keep_original=False)
        aug_dataset, aug_databases = generator.generate(dbs, dataset)
        dataset = aug_dataset

    hardness = {'easy': 0, 'medium': 0, 'hard': 0, 'extra': 0}
    evaluator = Evaluator()
    for data in dataset:
        if data['db_id'] == 'baseball_1':
            continue
        sql = data['sql']
        hn = evaluator.eval_hardness(sql)
        hardness[hn] += 1

    print(hardness)


def check_sql():
    from asdl.asdl import ASDLGrammar
    from asdl.transition_system import TransitionSystem
    from asdl.action_info import get_action_infos
    dataset = json.load(open('data/dev_aug.json', 'r'))
    GRAMMAR_FILEPATH = 'asdl/sql/grammar/sql_asdl_v2.txt'

    grammar = ASDLGrammar.from_filepath(GRAMMAR_FILEPATH)
    trans = TransitionSystem.get_class_by_lang('sql')(grammar)
    for data in dataset:
        sql = data['sql']
        ast = trans.surface_code_to_ast(sql)
        actions = trans.get_actions(ast)
        actions = get_action_infos(tgt_actions=actions)



if __name__ == '__main__':
    # mode = 'dev'
    # statistic_hardness(mode, use_gen=True, affected=False)
    check_sql()