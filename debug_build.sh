cd table/
g++ -g table_test.cc -o table_test ../build/libleveldb.a -lpthread -I../include -I../db