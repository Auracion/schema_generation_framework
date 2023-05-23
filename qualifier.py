import os, sys, json

sys.path.append(os.path.dirname(__file__))

from erg_utils import *
from database_utils import Database


def whitelist_for_e2a(entities):
    '''
    :param entities: Dict[entity.name: ERDNode]
    :return: List[(src_entity_name:str, tgt_entity_name:str)]
    '''

    nodes = {}
    for ent in entities:
        entity = entities[ent]
        nodes[ent] = {"forward": [], "backward": [], "relation": []}
        for adj in entity.adjacent:
            edges = entity.get_relation(adj)
            if adj.name not in nodes[ent]["forward"] and len(edges["forward"]) > 0:
                nodes[ent]["forward"].append(adj.name)
            if adj.name not in nodes[ent]["backward"] and len(edges["backward"]) > 0:
                nodes[ent]["backward"].append(adj.name)
            if adj.name not in nodes[ent]["relation"] and len(edges["relation"]) > 0:
                nodes[ent]["relation"].append(adj.name)

    white_list = []
    for src in nodes:
        if len(nodes[src]["relation"]) > 0:
            continue
        if len(nodes[src]["forward"]) == 1:
            if src == nodes[src]["forward"][0]:
                continue
            entity = entities[src]
            flag = True
            for column in entity.primary:
                if column not in entity.agents:
                    for path, branch in column.codes:
                        if "on" not in path:
                            flag = False
                            break
                if not flag:
                    break
            for tgt in nodes[src]["backward"]:
                target = entities[tgt]
                edges = entity.get_relation(target)['backward']
                for edge in edges:
                    if edge.end_joint.is_pk:
                        flag = False
                        break
                if not flag:
                    break

            if flag:
                white_list.append((src, nodes[src]["forward"][0]))

    return white_list


def whitelist_for_r2u(relation_edges):
    '''
    :param relation_edges: Dict[relation.name: ERGEdge]
    :return: List[(src_entity_name: str, tgt_entity_name: str)]
    '''
    white_list = []
    for rel in relation_edges:
        relation = relation_edges[rel]
        left_entity = relation.left
        right_entity = relation.right
        left_fks = relation.left_fks
        right_fks = relation.right_fks

        r2l = True
        for edge in left_fks:
            if not (edge.start.name == left_entity.table.name
                    and edge.end.name == relation.table.name):
                continue
            for path_code, branch_code in edge.end_joint.codes:
                if "on" not in path_code.path:
                    r2l = False
                    break

        l2r = True
        for edge in right_fks:
            if not (edge.start.name == right_entity.table.name and
                    edge.end.name == relation.table.name):  # unk
                continue
            for path_code, branch_code in edge.end_joint.codes:
                if "on" not in path_code.path:
                    l2r = False
                    break

        if r2l:
            white_list.append(('r2l', rel))
        if l2r:
            white_list.append(('l2r', rel))

    return white_list


def whitelist_for_c2a(entities):
    '''
    :param entity_nodes: entity_nodes: Dict[entity.name: ERDNode]
    :return:
    '''
    white_list = []
    all_tables = [k.lower() for k in entities]
    for ent in entities:
        tag = entities[ent].table.tag
        if tag == "PERSON":
            new_concept = "people"
            concept_column = "identity"
            if new_concept not in all_tables:
                white_list.append((ent, new_concept, concept_column))
        elif tag == "ORG":
            new_concept = "organization"
            concept_column = "type"
            if new_concept not in all_tables:
                white_list.append((ent, new_concept, concept_column))
        elif tag == "GPE":
            new_concept = "location"
            concept_column = "level"
            if new_concept not in all_tables:
                white_list.append((ent, new_concept, concept_column))

    return white_list


def whitelist_for_u2r(entities):
    '''
    :param unk_edges: List[ERDEdge]
    :return: List[src_entity_name: str, tgt_entity_name: str]
    '''
    white_list = []
    for src in entities:
        source = entities[src]
        for tgt in entities:
            if tgt == src:
                continue
            target = entities[tgt]
            if not source.has_adjacent(target):
                continue
            edges = source.get_relation(target)["forward"]
            if len(edges) == 0:
                continue
            flag = True
            for edge in edges:
                if edge.end_joint.is_pk:
                    flag = False
                    break
            if flag:
                white_list.append((src, tgt))
    return white_list


def filter_for_e2a(erg, used: bool):
    entities = erg.entities
    acdb = erg.acdb
    used_tables, unused_tables = [], []
    for table in acdb.tables:
        if table.dtype != 'Entity':
            continue
        if len(list(table.codes)) > 0:
            used_tables.append(table.name)
        else:
            unused_tables.append(table.name)

    whitelist = whitelist_for_e2a(entities)
    used_pairs, unused_pairs = [], []
    for src, tgt in whitelist:
        if src in used_tables:
            used_pairs.append((src, tgt))
        else:
            unused_pairs.append((src, tgt))

    if used:
        return used_pairs
    else:
        return unused_pairs


def filter_for_c2a(erg, used: bool):
    entities = erg.entities
    acdb = erg.acdb
    used_tables, unused_tables = [], []
    for table in acdb.tables:
        if table.dtype != 'Entity':
            continue
        if len(list(table.codes)) > 0:
            used_tables.append(table.name)
        else:
            unused_tables.append(table.name)

    whitelist = whitelist_for_c2a(entities)
    used_concepts, unused_concepts = [], []
    for ent, new_concept, concept_column in whitelist:
        if ent in used_tables:
            used_concepts.append((ent, new_concept, concept_column))
        else:
            unused_concepts.append((ent, new_concept, concept_column))

    if used:
        return used_concepts
    else:
        return unused_concepts


def filter_for_r2u(erg, used: bool):
    relation_edges = erg.relations
    acdb = erg.acdb
    used_tables, unused_tables = [], []
    for table in acdb.tables:
        if len(list(table.codes)) > 0:
            used_tables.append(table.name)
        else:
            unused_tables.append(table.name)

    whitelist = whitelist_for_r2u(relation_edges)
    used_triples, unused_triples = [], []
    # print(used_tables)
    # print(whitelist)
    for direction, rel in whitelist:
        # print(rel)
        if rel in used_tables:
            used_triples.append((direction, rel))
        else:
            unused_triples.append((direction, rel))
    # print()

    if used:
        return used_triples
    else:
        return unused_triples


def filter_for_u2r(erg, used: bool):
    entities = erg.entities
    acdb = erg.acdb
    used_tables, unused_tables = [], []
    for table in acdb.tables:
        if table.dtype != 'Entity':
            continue
        if len(list(table.codes)) > 0:
            used_tables.append(table.name)
        else:
            unused_tables.append(table.name)

    whitelist = whitelist_for_u2r(entities)
    used_pairs, unused_pairs = [], []
    for src, tgt in whitelist:
        if src in used_tables and tgt in used_tables:
            used_pairs.append((src, tgt))
        else:
            unused_pairs.append((src, tgt))

    if used:
        return used_pairs
    else:
        return unused_pairs