from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import json

MAX_RESULTS = 50

class GraphQLClient:
    def __init__(self, config):
        self.api_key = config['api_key']
        self.base_url = config['base_url']
        sample_transport=RequestsHTTPTransport(
            url=self.base_url,
            use_json=True,
            headers={
                "Content-type": "application/json",
                "Authorization": self.api_key
            },
            verify=False
        )
        self.client = Client(
            retries=3,
            transport=sample_transport,
            fetch_schema_from_transport=True,
        )
    def get(self, name, schema, is_singular=False):
        # `is_singular` is a way to deal with Linear's implementation of 'connections'.
        done = False
        after_id = None
        final_result = []
        while not done:
            query = gql(self.schema_to_gql(name, schema, is_singular, after_id))
            res = self.client.execute(query)
            if is_singular:
                final_result = [res[name]]
                done = True 
            else:
                final_result = final_result + res[name]['nodes']
                if len(res[name]['nodes']) >= MAX_RESULTS:
                    after_id = res[name]['nodes'][-1]['id']
                else:
                    done = True
        return final_result
    def schema_to_gql(self, root_name, schema, is_singular, after_id=None):
        def properties_to_gql(name, value, is_singular):
            if 'properties' in value:
                children = ''
                for k,v in value['properties'].items():
                    # determining if this object will be a `connection` in Linear's API.
                    if len(v["type"]) > 1 and v["type"][1] == "array" and "items" in v:
                        children += f'{properties_to_gql(k, v["items"], False)} '
                    else:
                        children += f'{properties_to_gql(k, v, True)} '
                # hack to deal with Linear's implementation of 'connections'.
                if is_singular:
                    children = f'{{ {children} }}'
                else:
                    children = f'{{ nodes {{ {children} }} }}'
                if after_id and name == root_name:
                    name += f'(first: {MAX_RESULTS},after: "{after_id}")'
                return f'{name} {children}'
            else:
                return f'{name} '
        gql_properties = properties_to_gql(root_name, schema, is_singular)
        gql_string = f'''query {{ {gql_properties} }}'''
        return gql_string
    def get_join_through_data(self, name, join_data):
        query_string = f'''query {{
            {join_data["tables"][0]}  {{
                nodes {{
                    id
                    {join_data["field"]} {{
                        nodes {{
                            id
                        }}
                    }}
                }}
            }}
        }}'''
        query = gql(query_string)
        res = self.client.execute(query)
        table_data = []
        for node in res[join_data["tables"][0]]['nodes']:
            for related_node in node[join_data['field']]['nodes']:
                table_data.append({
                    f'{join_data["tables"][0][:-1]}_id': node['id'],
                    f'{join_data["tables"][1][:-1]}_id': related_node['id'],
                })
        return table_data