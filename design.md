# Design 

## Modules

- `Codec`: value encode/decode
- `Watcher`: loop watch control
- `Handler`: include `ValueHandler` and `MapHandler`, set/get value, handle event, synchronize value, trigger listener
- `Listener`:  include `ValueListener` and `ChildListener`,  listen value change

## API

- `Sync*`:  synchronize data into memory
- `SyncWatch*`:  synchronize data into memory, and listen value change
- `Decode*`: set value into zookeeper
- `Encode*`: get value from zookeeper