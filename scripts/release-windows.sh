#!/bin/sh
if [ "$GITHUB_ACTIONS" == "true" ]; then
	PATH=/ucrt64/bin:$PATH:/c/Users/Default/.cargo/bin
	pacman -S --noconfirm make zip git mingw-w64-ucrt-x86_64-gcc
fi
make -C LuaJIT amalg -j4
(cd fhk5 && RUSTFLAGS='-L ../LuaJIT/src' cargo build --release -F trace --target x86_64-pc-windows-gnu)
strip fhk5/target/x86_64-pc-windows-gnu/release/fhk.dll
make M3EXE_FHK=fhk5/target/x86_64-pc-windows-gnu/release/fhk.dll -j4
zip -j m3-$(git describe)-x64-windows.zip m3.exe LuaJIT/src/lua51.dll fhk5/target/x86_64-pc-windows-gnu/release/fhk.dll
