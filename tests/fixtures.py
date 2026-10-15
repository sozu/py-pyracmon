from pyracmon.model import Column, Table, Relations, Model


#----------------------------------------------------------------
# Dummy models.
#----------------------------------------------------------------
COLUMN = NotImplemented

table1 = Table("t1", [
    Column("c1", int, None, True, None, "seq", False, "c1 in t1"),
    Column("c2", int, None, False, Relations(), None, False, "c2 in t1"),
    Column("c3", int, None, False, None, None, True, "c3 in t1"),
])


table2 = Table("t2", [
    Column("c1", int, None, True, None, "seq", False),
    Column("c2", int, None, True, Relations(), None, True),
    Column("c3", int, None, False, None, None, False),
])


table3 = Table("t3", [
    Column("c1", int, None, False, None, "seq", False),
    Column("c2", int, None, False, Relations(), None, False),
    Column("c3", int, None, False, None, None, False),
])