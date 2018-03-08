class MockConnection():
    appid = "database_id"

    def get_tables(self):
        return [
            {
                'id': '1',
                'name': 'table_name',
                'database_name': 'database_name',
                'database_id': 'database_id',
            }
        ]

    def get_fields(self, table_id):
        fields = {'1': [
            {
                'id': '1',
                'name': 'datecreated',
                'type': 'timestamp',
                'base_type': 'int64',
                'parent_field_id': '',
            },
            {
                'id': '2',
                'name': 'datemodified',
                'type': 'timestamp',
                'base_type': 'int64',
                'parent_field_id': '',
            },
            {
                'id': '3',
                'name': 'text_field',
                'type': 'text',
                'base_type': 'text',
                'parent_field_id': '',
            },
            {
                'id': '4',
                'name': 'boolean_field',
                'type': 'checkbox',
                'base_type': 'bool',
                'parent_field_id': '',
            },
            {
                'id': '5',
                'name': 'float_field',
                'type': 'float',
                'base_type': 'float',
                'parent_field_id': '',
            },
            {
                'id': '6',
                'name': 'child_text_field',
                'type': 'text',
                'base_type': 'float',
                'parent_field_id': '3',
            },
        ]}
        return fields[table_id]
