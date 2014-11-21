import operator
from mockthink.ast import *
from mockthink import db
from mockthink.rql_rewrite import rewrite_query
import rethinkdb as r
from pprint import pprint

query1 = FilterWithFunc(
    RTable(
        RDb(RDatum('fonz')),
        RDatum('wabbits')
    ),
    RFunc(
        ['x'],
        Gt(
            Bracket(RVar(RDatum('x')), RDatum('age')),
            RDatum(20)
        )
    )
)

query2 = Get(
    RTable(
        RDb(RDatum('fonz')),
        RDatum('wabbits')
    ),
    RDatum('joe-id'),
)

query3 = FilterWithObj(
    RTable(
        RDb(RDatum('fonz')),
        RDatum('wabbits')
    ),
    {'name': 'smith'}
)

query4 = UpdateWithObj(
    RTable(
        RDb(RDatum('fonz')),
        RDatum('wabbits')
    ),
    {
        'is_wabbit': True
    }
)


query5 = MapWithRFunc(
    RTable(
        RDb(RDatum('fonz')),
        RDatum('wabbits')
    ),
    RFunc(
        ['x'],
        Gt(
            Bracket(RVar(RDatum('x')), RDatum('age')),
            RDatum(20)
        )
    )
)

query6 = WithoutMap(
    RTable(
        RDb(RDatum('fonz')),
        RDatum('wabbits')
    ),
    ['age']
)


query7 = RTable(
    RDb(RDatum('fonz')),
    RDatum('wabbits')
)

data = {
    'dbs': {
        'fonz': {
            'tables': {
                'wabbits': [
                    {'id': 'steve-id', 'name': 'steve', 'age': 26},
                    {'id': 'joe-id', 'name': 'joe', 'age': 15},
                    {'id': 'todd-id', 'name': 'todd', 'age': 65},
                    {'id': 'smith-id', 'name': 'smith', 'age': 34},
                    {'id': 'tim-id', 'name': 'tim', 'age': 19}
                ]
            }
        }
    }
}

mockthink = db.MockThink(data)
# for query in [query1, query2, query3, query4, query5, query6, query7]:
#     pprint(mockthink.run_query(query))

# mockthink.reset()
# pprint(mockthink.run_query(query7))

rql1 = r.db('fonz').table('wabbits').filter(lambda d: d['age'] > 26)
rql2 = r.db('fonz').table('wabbits')

another = r.db('fonz').table('wabbits').update({
    'is_wabbit': 'definitely'
})

query = rewrite_query(rql1)
# mockthink.pprint_query_ast(query)
pprint(mockthink.run_query(query))
pprint(mockthink.run_query(rewrite_query(another)))
pprint(mockthink.run_query(rewrite_query(rql2)))

mockthink.reset()

pprint(mockthink.run_query(rewrite_query(rql2)))
