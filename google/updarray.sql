insert into testArray(id,intarray,strarray,numarray)
values (0,[100],["abc","def"],generate_array(1.0,10.0,2.25));



insert into testArray(id,intarray)
values (1,generate_array(200,200+25,5))
