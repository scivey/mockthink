# MockThink

MockThink is an in-process Python clone of RethinkDB's API.  For testing.  It's not done yet.

## Usage

```python
    from pprint import pprint
    from mockthink import MockThink
    import rethinkdb as r

    db = MockThink({
        'dbs': {
            'tara': {
                'tables': {
                    'people': [
                        {'id': 'john-id', 'name': 'John'},
                        {'id': 'sam-id', 'name': 'Sam'}
                    ]
                }
            }
        }
    })

    with db.connect() as conn:
        result = r.db('tara').table('people').map(
            lambda doc: doc.merge({'also_name': doc['name']})
        ).run(conn)
        pprint(list(result))

        # [
        #    {'also_name': 'John', 'id': 'john-id', 'name': 'John'},
        #    {'also_name': 'Sam', 'id': 'sam-id', 'name': 'Sam'}
        # ]

        r.db('tara').table('people').update(
            {'likes_fonz': True}
        ).run(conn)

        result = r.db('tara').table('people').run(conn)
        pprint(list(result))

        # [
        #    {'id': 'john-id', 'likes_fonz': True, 'name': 'John'},
        #    {'id': 'sam-id', 'likes_fonz': True, 'name': 'Sam'}
        # ]

    # data is reset at exit of context manager above

    with db.connect() as conn:
        result = r.db('tara').table('people').run(conn)
        pprint(list(result))
        # [
        #    {'id': 'john-id', 'name': 'John'},
        #    {'id': 'sam-id', 'name': 'Sam'}
        # ]
```
