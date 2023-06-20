from panos.objects import AddressObject, Tag
import random
import uuid
from datetime import datetime
import re

tag_list = [Tag(name='TAG-' + str(x)) for x in range(6)]
tags_sets = dict()
nb_objects = 2000

def print_object(obj):
    print(f"{obj.name} - {obj.value} - {obj.tag}")

def generate_random_object():
    ip_addr = ".".join([str(random.randint(1, 255)) for x in range(4)])
    name = str(uuid.uuid4())
    tags = set()
    for _ in range(random.randint(0,5)):
        tags.add(str(tag_list[random.randint(0, len(tag_list) - 1)]))
    return AddressObject(name=name, value=ip_addr, tag=list(tags))

start_objects_init = datetime.now()
print(f"Start generating {nb_objects} random objects... ")
address_objects = [generate_random_object() for _ in range(nb_objects)]
end_objects_init = datetime.now()

print(f"OK Took {end_objects_init - start_objects_init}")

print("Populating tags sets... ")
for o in address_objects:
    for t in o.tag:
        if t not in tags_sets:
            tags_sets[t] = set()
        tags_sets[t].add(o)

end_tags_set_init = datetime.now()
print(f"OK Took {end_tags_set_init - end_objects_init}\n\n")

def find_with_list_iteration(condition_expression, print_count=True):
    # changes a DAG condition expression into an executable Python statement
    # checking presence of the tags into the obj.tag list
    # ie : ("TAG-1" and "TAG-2") or "TAG-3" becomes
    # cond_expr_result = ('TAG-1' in obj.tag and 'TAG-2' in obj.tag) or 'TAG-3' in obj.tag

    condition_expression = condition_expression.replace('\"', '\'')
    exec_statement = "cond_expr_result = " + re.sub("'((\w|[-:\+])+)'", rf"'\1' in obj.tag", condition_expression)
    #print(f"Python statement is : {exec_statement}")
    # this part iterates over the objects list and runs the condition expression on each object
    # getting the output (True or False), and adding the object to the matched list accordingly
    matched_objects = set()
    for obj in address_objects:
        exec_result = dict()
        exec(exec_statement, {'obj': obj}, exec_result)
        if exec_result.get('cond_expr_result') is True:
            matched_objects.add(obj)
    if print_count:
        print(f"{len(matched_objects)} found")
    return len(matched_objects)

def find_with_sets(condition_expression, print_count=False):
    # changes a DAG condition expression into an executable Python statement
    # in the form of a sets intersection / symmetric difference
    # ie : ("TAG-1" and "TAG-2") or "TAG-3" becomes
    # cond_expr_result = (tags_sets.get('TAG-1', set()) & tags_sets.get('TAG2', set())) ^ tags_sets.get('TAG3', set())

    condition_expression = condition_expression.replace('and', '&')
    condition_expression = condition_expression.replace('or', '|')
    condition_expression = condition_expression.replace('AND', '&')
    condition_expression = condition_expression.replace('OR', '|')
    # remove all quotes from the logical expression
    condition_expression = condition_expression.replace('\'', '')
    condition_expression = condition_expression.replace('\"', '')
    condition_expression = re.sub("((\w|-|:|\+)+)", rf"tags_sets.get('\1', set())", condition_expression)
    exec_statement = "cond_expr_result = " + condition_expression
    #print(f"Python statement is : {exec_statement}")
    # this part executes the generated Python statement, and thus gets the list of matched objects
    # from the tags_set content
    exec_result = dict()
    # the statement needs to be ran in the globals() scope to have access to the tags_set dict in this demo context
    exec(exec_statement, {'tags_sets': tags_sets}, exec_result)
    if exec_result.get('cond_expr_result'):
        if print_count:
            print(f"{len(exec_result.get('cond_expr_result'))} found")
        return len(exec_result.get('cond_expr_result'))
    return 0


def gen_random_dag_string():
    operators = {0: "and", 1: "or"}
    max_groups = 3
    max_tags_per_group = 3

    condition = ""
    for i in (last := range(random.randint(1, max_groups))):
        condition += "("
        for j in (last2 := range(random.randint(1, max_tags_per_group))):
            condition += f"'{tag_list[random.randint(0, len(tag_list)-1)]}' "
            condition += operators[random.randint(0, len(operators)-1)] + " " if j < last2[-1] else ""
        condition += ")"
        condition += " " + operators[random.randint(0, len(operators)-1)] + " " if i < last[-1] else ""

    return condition

while True:
    try:
        condition_expression = input("DAG expression : ")
        if condition_expression == "exit" or condition_expression == "":
            break
        if condition_expression.isdigit():
            nb_random_conditions = int(condition_expression)
            print(f"Generating {nb_random_conditions} random DAG conditions...")
            random_conditions = [gen_random_dag_string() for _ in range(int(nb_random_conditions))]

            print(f"Resolving DAG members on {nb_random_conditions} DAGs with iterative method...")
            start_find_iterative = datetime.now()
            matched_total = 0
            for random_cond in random_conditions:
                if nb_random_conditions <= 10:
                    print(random_cond)
                matched_total += find_with_list_iteration(random_cond, print_count=False if nb_random_conditions > 10 else True)
            end_find_iterative = datetime.now()
            print(f"OK Took {end_find_iterative - start_find_iterative}")
            print(f"Total : {matched_total} objects matched")
            print("\n")

            print(f"Resolving DAG members on {nb_random_conditions} DAGs with sets...")
            matched_total = 0
            for random_cond in random_conditions:
                if nb_random_conditions <= 10:
                    print(random_cond)
                matched_total += find_with_sets(random_cond, print_count=False if nb_random_conditions > 10 else True)
            end_find_sets = datetime.now()
            print(f"OK Took {end_find_sets - end_find_iterative}")
            print(f"Total : {matched_total} objects matched")
            print("\n\n")
        else:
            print("Start search with iterative lookup")
            start_find_iterative = datetime.now()
            find_with_list_iteration(condition_expression)
            end_find_iterative = datetime.now()

            print(f"OK Took {end_find_iterative - start_find_iterative}")

            print("\n")
            print("Start search with sets")
            find_with_sets(condition_expression)
            end_find_sets = datetime.now()

            print(f"OK Took {end_find_sets - end_find_iterative}")
            print("\n")
    except Exception as e:
        print(e)
        pass
