## Examples/Tests

Set up with `st-geom-onerow.txt` and `st-geom-aggr.txt`.

```sh
hive -S -f st-geom-desc.sql >& st-geom-desc.out
 diff -q st-geom-desc.ref st-geom-desc.out
hive -S -f st-geom-text.sql >& st-geom-text.out
 diff -wq st-geom-text.ref st-geom-text.out
hive -S -f st-geom-exact.sql >& st-geom-exact.out
 diff -q st-geom-exact.ref st-geom-exact.out
hive -S -f st-geom-bins.sql >& st-geom-bins.out
 diff -wq st-geom-bins.ref st-geom-bins.out
hive -S -f st-geom-multi-call.sql >& st-geom-multi-call.out
 diff -q st-geom-multi-call.ref st-geom-multi-call.out
hive -S -f st-geom-aggr.sql >& st-geom-aggr.out
 diff -q st-geom-aggr.ref st-geom-aggr.out
```
