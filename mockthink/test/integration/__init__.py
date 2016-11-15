from __future__ import print_function


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
