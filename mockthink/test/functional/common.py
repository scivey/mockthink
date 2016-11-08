from __future__ import absolute_import, division, print_function, unicode_literals


class MockTest(object):
    @staticmethod
    def get_data():
        return {
            'dbs': {
                'default': {
                    'tables': {}
                }
            }
        }

