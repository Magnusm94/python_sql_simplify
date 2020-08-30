# This function builds insert functions for sql.
def insert_command(tablename, **kwargs):
	joinkeys = lambda dictionary: ", ".join(str(key) for key in dictionary.keys())
	joinvalues = lambda dictionary: ", ".join(str(value) for value in dictionary.values())
	command = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, joinkeys(kwargs), joinvalues(kwargs))
	return command
 
 
"""
Examples:
insert_command('my_table', name=foo, lastname=bar, age=25)
>> INSERT INTO my_table (name, lastname, age) VALUES (foo, bar, 25)


It can also take dictionaries:

data = {
  'name': 'foo',
  'lastname': 'bar',
  'age': 25
}

insert_command('my_table', **data)
>> INSERT INTO my_table (name, lastname, age) VALUES (foo, bar, 25)

"""
