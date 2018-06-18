class MockConnection():
    appid = "app_id"

    def get_tables(self):
        return [
            {
                'id': '1',
                'name': 'table_name',
                'app_name': 'app_name',
                'app_id': 'app_id',
            }
        ]

    def get_fields(self, table_id):
        fields = {'1': {
            '1': {
                'id': '1',
                'name': 'datecreated',
                'type': 'timestamp',
                'base_type': 'int64',
                'parent_field_id': '',
            },
            '2': {
                'id': '2',
                'name': 'datemodified',
                'type': 'timestamp',
                'base_type': 'int64',
                'parent_field_id': '',
            },
            '3': {
                'id': '3',
                'name': 'text_field',
                'type': 'text',
                'base_type': 'text',
                'parent_field_id': '',
            },
            '4': {
                'id': '4',
                'name': 'boolean_field',
                'type': 'checkbox',
                'base_type': 'bool',
                'parent_field_id': '',
            },
            '5': {
                'id': '5',
                'name': 'float_field',
                'type': 'float',
                'base_type': 'float',
                'parent_field_id': '',
            },
            '6': {
                'id': '6',
                'name': 'child_text_field',
                'type': 'text',
                'base_type': 'float',
                'parent_field_id': '7',
            },
            '7': {
                'id':'7',
                'name': 'parent_field',
                'type': 'text',
                'base_type': 'text',
                'parent_field_id': '',
                'composite_fields': ['6'],
            }
        }}
        return fields[table_id]
