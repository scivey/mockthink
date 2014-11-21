import operator
from mockthink.ast import *
from mockthink import db
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
pprint(mockthink.run_query(query1))
# pprint(query2.run(data, Scope({})))
# pprint(query3.run(data, Scope({})))
# pprint(query4.run(data, Scope({})))

# pprint(query1.run(data, Scope({})))
# pprint(query5.run(data, Scope({})))
# pprint(query6.run(data, Scope({})))
