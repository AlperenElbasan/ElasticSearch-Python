# https://towardsdatascience.com/elasticsearch-tutorial-for-beginners-using-python-b9cb48edcedc

from elasticsearch import Elasticsearch  # this is elasticsearch-py low level library.
from elasticsearch_dsl import Search, \
    UpdateByQuery  # elasticsearch-dsl can be used for high level API usage against elasticsearch.
import time
import sys

HOST_URLS = ["http://127.0.0.1:9200"]
es = Elasticsearch(HOST_URLS)


def get_cluster_name():
    return es.cluster.state(metric=['cluster_name'])


def add_employee(body, doc_type="_doc", desired_id=0, index="megacorp"):
    res = es.index(index=index, doc_type=doc_type, id=id, body=e1)
    time.sleep(1)
    print("Add employee: " + res['result'])


def remove_employee(id, doc_type="_doc", index="megacorp"):
    res = es.delete(index=index, doc_type=doc_type, id=id)
    time.sleep(1)
    print("Remove employee: " + res['result'])


def get_all_employees(index='megacorp'):
    res = es.search(index=index, body={'query': {'match_all': {}}})
    return res['hits']['total']['value'], res['hits']['hits']


def print_all_employees():
    count, hits = get_all_employees()
    print_res(count=count, hits=hits)


def get_employees(key, value, index='megacorp'):
    res = es.search(index=index, body={
        'query': {
            'match': {
                key:
                    value
            }
        }
    })
    return res['hits']['total']['value'], res['hits']['hits']


def get_prefix_employee(key, value, index='megacorp'):
    res = es.search(index=index, body={
        'query': {
            'match_phrase_prefix': {
                key: {
                    "query": value
                }
            }
        }
    })
    return res['hits']['total']['value'], res['hits']['hits']


def match_phrase(value, index='megacorp'):
    res = es.search(index=index, doc_type='employee', body={
        'query': {
            'match_phrase': {
                "about": value
            }
        }
    })
    return res['hits']['total']['value'], res['hits']['hits']


def update_employee_info(id, body, index="megacorp", doc_type="employee"):
    res = es.update(index=index, doc_type=doc_type, id=id, body=body)
    time.sleep(1)
    print("Result of partial update: " + res['result'])


def partial_update_employee_info(id, key, value, index='megacorp', doc_type='employee'):
    res = es.update(index="megacorp", doc_type="employee", id=id, body={
        "doc": {
            key: value
        }
    })
    return "Result of partial update: " + res['result']


def add_array_value_to_employee(id, array_name, new_value, index="megacorp"):
    try:
        res = es.get(index=index, doc_type="employee", id=id)
    except:
        return str("No such document with id = " + str(id))

    hit = res["_source"]
    array = hit[array_name]
    if array is not None:
        res = es.update(index=index, doc_type="_doc", id=id, body={
            "doc": {
                "interests": [str(new_value)]
            }
        })
    else:
        array.append({new_value})
        res = es.update(index=index, doc_type="_doc", id=id, body={
            "doc": {
                "interests": array
            }
        })
    print("Adding " + new_value + " to " + array_name + "is : " + res['result'])


def change_numeric_value(id, key, value, index="megacorp", doc_type="employee"):
    try:
        res = es.get(index=index, doc_type=doc_type, id=id)
        if res is not None:
            employee = res['_source']
        old_value = employee[key]
        new_value = old_value + value
        print(partial_update_employee_info(id=id, key=key, value=new_value))
    except TypeError:
        print('Value is not numeric: ' + str(sys.exc_info()))
    except KeyError:
        print(key + ' not exists in instance with id ' + str(id) + ": " + str(sys.exc_info()))


def print_res(count, hits, mes="Request"):
    print(mes + ' got %d hits:' % count)
    for hit in hits:
        print("\tID %(_id)s : %(_source)s" % hit)


e1 = {
    "first_name": "Ali",
    "last_name": "panwar",
    "age": 57,
    "about": "Love to play cricket",
    "interests": ['yoga', 'music'],
}
e2 = {
    "first_name": "ayşe",
    "last_name": "Smith",
    "age": 22,
    "about": "I like to collect rock albums",
    "interests": ["hot yoga"]
}
e3 = {
    "first_name": "merve Eren",
    "last_name": "Fir",
    "age": 21,
    "about": "I like to build cabinets",
    "interests": ["sports"]
}

# add_employee(body=e1)
# add_employee(body=e2)
# add_employee(body=e3)

print_all_employees()

# add_array_value_to_employee(id=1, array_name='interests', new_value="Professional Flirting")
# change_numeric_value(id=1, key="salary", value=10)

count, hits = get_prefix_employee("first_name", "Alp")
print_res(count, hits, mes="Employees with name Alp")
for hit in hits:
    partial_update_employee_info(id=hit['_id'], key="first_name", value="Alpella")
time.sleep(1)
print_all_employees()
