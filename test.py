from dynamodb_meta_store import DynamoDBMetaStore
from dynamodb_meta_store.exceptions import ItemNotFound, MisconfiguredSchemaException

import unittest
import boto3


connection = boto3.resource("dynamodb", endpoint_url="http://localhost:8000")


class TestCustomThroughput(unittest.TestCase):

    def setUp(self):

        # Configuration options
        self.table_name = "test"
        self.store_name = "test"
        self.read_units = 10
        self.write_units = 8

        # Instanciate the store
        self.store = DynamoDBMetaStore(
            connection=connection,
            table_name=self.table_name,
            store_name=self.store_name,
            read_units=self.read_units,
            write_units=self.write_units,
            create_table=True
        )

        # Get an Table instance for validation
        self.table = self.store.table

    def test_throughput(self):
        """ Test that we have correct throughput for new table """
        throughput = self.table.provisioned_throughput

        self.assertEqual(throughput["ReadCapacityUnits"], self.read_units)
        self.assertEqual(throughput["WriteCapacityUnits"], self.write_units)

    def tearDown(self):
        """ Tear down the test case """
        self.table.delete()


class TestCustomStoreAndOptionKeys(unittest.TestCase):

    def setUp(self):

        # Configuration options
        self.table_name = "test"
        self.store_name = "test"
        self.store_key = "_s"
        self.option_key = "_o"

        # Instanciate the store
        self.store = DynamoDBMetaStore(
            connection=connection,
            table_name=self.table_name,
            store_name=self.store_name,
            store_key=self.store_key,
            option_key=self.option_key,
            create_table=True
        )

        # Get an Table instance for validation
        self.table = self.store.table

    def test_custom_store_and_option_keys(self):
        """ Test that we can set custom store and option keys """
        obj = {
            "host": "127.0.0.1",
            "port": 27017
        }

        # Insert the object
        self.store.set("db", obj)

        # Fetch the object directly from DynamoDB
        key = {
            "_s": self.store_name,
            "_o": "db"
        }
        response = self.table.get_item(Key=key)
        item = response["Item"]

        self.assertEqual(item["_s"], self.store_name)
        self.assertEqual(item["_o"], "db")
        self.assertEqual(item["host"], "127.0.0.1")
        self.assertEqual(item["port"], 27017)

    def tearDown(self):
        """ Tear down the test case """
        self.table.delete()


class TestDefaultThroughput(unittest.TestCase):

    def setUp(self):

        # Configuration options
        self.table_name = "test"
        self.store_name = "test"

        # Instanciate the store
        self.store = DynamoDBMetaStore(
            connection=connection,
            table_name=self.table_name,
            store_name=self.store_name,
            create_table=True
        )

        # Get an Table instance for validation
        self.table = self.store.table

    def test_throughput(self):
        """ Test that we have correct throughput for new table """
        throughput = self.table.provisioned_throughput

        self.assertEqual(throughput["ReadCapacityUnits"], 1)
        self.assertEqual(throughput["WriteCapacityUnits"], 1)

    def tearDown(self):
        """ Tear down the test case """
        self.table.delete()


class TestGetOption(unittest.TestCase):

    def setUp(self):

        # Configuration options
        self.table_name = "test"
        self.store_name = "test"

        # Instanciate the store
        self.store = DynamoDBMetaStore(
            connection=connection,
            table_name=self.table_name,
            store_name=self.store_name,
            create_table=True
        )

        # Get an Table instance for validation
        self.table = self.store.table

    def test_get(self):
        """ Test that we can retrieve an object from the store """
        obj = {
            "endpoint": "http://test.com",
            "port": 80,
            "username": "test",
            "password": "something"
        }

        # Insert the object
        self.store.set("api", obj)

        # Retrieve the object
        option = self.store.get("api")

        self.assertNotIn("_store", option)
        self.assertNotIn("_option", option)
        self.assertEqual(option["endpoint"], obj["endpoint"])
        self.assertEqual(option["port"], obj["port"])
        self.assertEqual(option["username"], obj["username"])
        self.assertEqual(option["password"], obj["password"])

    def test_get_item_not_found(self):
        """ Test that we can't retrieve non-existing items """
        with self.assertRaises(ItemNotFound):
            self.store.get("doesnotexist")

    def tearDown(self):
        """ Tear down the test case """
        self.table.delete()


class TestGetOptionAndKeysSubset(unittest.TestCase):

    def setUp(self):

        # Configuration options
        self.table_name = "test"
        self.store_name = "test"

        # Instanciate the store
        self.store = DynamoDBMetaStore(
            connection=connection,
            table_name=self.table_name,
            store_name=self.store_name,
            create_table=True
        )

        # Get an Table instance for validation
        self.table = self.store.table

    def test_get(self):
        """ Test that we can retrieve an object from the store """
        obj = {
            "endpoint": "http://test.com",
            "port": 80,
            "username": "test",
            "password": "something"
        }

        # Insert the object
        self.store.set("api", obj)

        # Retrieve the object
        option = self.store.get("api", keys=["endpoint", "port"])

        self.assertNotIn("_store", option)
        self.assertNotIn("_option", option)
        self.assertNotIn("username", option)
        self.assertNotIn("password", option)
        self.assertEqual(option["endpoint"], obj["endpoint"])
        self.assertEqual(option["port"], obj["port"])

    def tearDown(self):
        """ Tear down the test case """
        self.table.delete()


class TestGetFullStore(unittest.TestCase):

    def setUp(self):

        # Configuration options
        self.table_name = "test"
        self.store_name = "test"

        # Instanciate the store
        self.store = DynamoDBMetaStore(
            connection=connection,
            table_name=self.table_name,
            store_name=self.store_name,
            create_table=True
        )

        # Get an Table instance for validation
        self.table = self.store.table

    def test_get_of_full_store(self):
        """ Test that we can retrieve all objects in the store """
        objApi = {
            "endpoint": "http://test.com",
            "port": 80,
            "username": "test",
            "password": "something"
        }
        objUser = {
            "username": "luke",
            "password": "skywalker"
        }

        # Insert the object
        self.store.set("api", objApi)
        self.store.set("user", objUser)

        # Retrieve all objects
        options = self.store.get()
        self.assertEqual(len(options), 2)
        optApi = options["api"]
        optUser = options["user"]

        self.assertNotIn("_store", optApi)
        self.assertNotIn("_option", optApi)
        self.assertEqual(optApi["endpoint"], objApi["endpoint"])
        self.assertEqual(optApi["port"], objApi["port"])
        self.assertEqual(optApi["username"], objApi["username"])
        self.assertEqual(optApi["password"], objApi["password"])

        self.assertNotIn("_store", optUser)
        self.assertNotIn("_option", optUser)
        self.assertEqual(optUser["username"], objUser["username"])
        self.assertEqual(optUser["password"], objUser["password"])

    def tearDown(self):
        """ Tear down the test case """
        self.table.delete()


class TestMisconfiguredSchemaException(unittest.TestCase):

    def setUp(self):

        # Configuration options
        self.table_name = "test"
        self.store_name = "test"

        # Instanciate the store
        self.store = DynamoDBMetaStore(
            connection=connection,
            table_name=self.table_name,
            store_name=self.store_name,
            create_table=True
        )

        # Get an Table instance for validation
        self.table = self.store.table

    def test_misconfigured_schema_store_key(self):
        """ Test that an exception is raised if the store key is not an hash """
        with self.assertRaises(MisconfiguredSchemaException):
            DynamoDBMetaStore(
                connection=connection,
                table_name=self.table_name,
                store_name=self.store_name,
                store_key="test",
                create_table=True
            )

    def test_misconfigured_schema_option_key(self):
        """ Test that an exception is raised if the option key isn't a range """
        with self.assertRaises(MisconfiguredSchemaException):
            DynamoDBMetaStore(
                connection=connection,
                table_name=self.table_name,
                store_name=self.store_name,
                option_key="test",
                create_table=True
            )

    def tearDown(self):
        """ Tear down the test case """
        self.table.delete()


class TestSet(unittest.TestCase):

    def setUp(self):

        # Configuration options
        self.table_name = "test"
        self.store_name = "test"

        # Instanciate the store
        self.store = DynamoDBMetaStore(
            connection=connection,
            table_name=self.table_name,
            store_name=self.store_name,
            create_table=True
        )

        # Get an Table instance for validation
        self.table = self.store.table

    def test_set(self):
        """ Test that we can insert an object """
        obj = {
            "host": "127.0.0.1",
            "port": 27017
        }

        # Insert the object
        self.store.set("db", obj)

        # Fetch the object directly from DynamoDB
        key = {
            "_store": self.store_name,
            "_option": "db"
        }
        response = self.table.get_item(Key=key)

        item = response["Item"]

        self.assertEqual(item["_store"], self.store_name)
        self.assertEqual(item["_option"], "db")
        self.assertEqual(item["host"], "127.0.0.1")
        self.assertEqual(item["port"], 27017)

    def test_update(self):
        """ Test that we can change values in an option """
        obj = {
            "username": "luke",
            "password": "skywalker"
        }

        # Insert the object
        self.store.set("user", obj)

        # Get the option
        option = self.store.get("user")
        self.assertEqual(option["username"], obj["username"])
        self.assertEqual(option["password"], obj["password"])

        # Updated version of the object
        updatedObj = {
            "username": "anakin",
            "password": "skywalker"
        }

        # Insert the object
        self.store.set("user", updatedObj)

        # Get the option
        option = self.store.get("user")
        self.assertEqual(option["username"], updatedObj["username"])
        self.assertEqual(option["password"], updatedObj["password"])

    def test_update_with_new_keys(self):
        """ Test that we can completely change the keys """
        obj = {
            "username": "luke",
            "password": "skywalker"
        }

        # Insert the object
        self.store.set("credentials", obj)

        # Get the option
        option = self.store.get("credentials")
        self.assertEqual(option["username"], obj["username"])
        self.assertEqual(option["password"], obj["password"])

        # Updated version of the object
        updatedObj = {
            "access_key": "anakin",
            "secret_key": "skywalker"
        }

        # Insert the object
        self.store.set("credentials", updatedObj)

        # Get the option
        option = self.store.get("credentials")
        self.assertEqual(option["access_key"], updatedObj["access_key"])
        self.assertEqual(option["secret_key"], updatedObj["secret_key"])
        self.assertNotIn("username", option)
        self.assertNotIn("password", option)

    def tearDown(self):
        """ Tear down the test case """
        self.table.delete()
        del self.store


if __name__ == "__main__":
    unittest.main()
