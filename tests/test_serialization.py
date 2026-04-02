
def test_node_password_serialization(dummy_node):
    node = Node(protocol='http', address='localhost', port=8080, username='user', password='pass')
    serialized = node.serialize()
    assert serialized['password'] == 'pass'
    deserialized = Node.deserialize(serialized)
    assert deserialized._api_helper.password == 'pass'

