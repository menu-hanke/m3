#!/bin/sh
make deps-luajit deps-sqlite -j4
(cd fhk5 && PATH=$PATH:/c/Users/Default/.cargo/bin RUSTFLAGS='-L ../LuaJIT/src' cargo build --release -F trace --target x86_64-pc-windows-gnu)
strip fhk5/target/x86_64-pc-windows-gnu/release/fhk.dll
make M3EXE_FHK=fhk5/target/x86_64-pc-windows-gnu/release/fhk.dll -j4
zip -j m3-$(git describe)-x64-windows.zip m3.exe m3.dll LuaJIT/src/lua51.dll fhk5/target/x86_64-pc-windows-gnu/release/fhk.dll
