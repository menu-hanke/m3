## m3

*m3* is a simulation platform based on [fhk](https://github.com/menu-hanke/fhk),
[LuaJIT](https://luajit.org), and [SQLite](https://sqlite.org).
*m3* lets you write Lua scripts to control fhk simulation models (which can
be implemented using any combination of fhk's
[supported languages](https://github.com/menu-hanke/fhk#building)).

## Example
First, you'll need an *m3* binary. Windows users can grab a prebuilt binary [here](https://github.com/menu-hanke/m3/releases). Otherwise, see [build instructions](#building).

Let's simulate tree growth using the growth models by
[Nyyssönen and Mielikäinen (1978)](https://doi.org/10.14214/aff.7597).
We'll put the *model library* in `models.fhk`:

```
table stand
table tree[N]

# Tree-level auxiliary variables
model tree {
    g = (d/2)^2 * 3.14
}

# Stand-level aggregates
model stand {
    G = sum(tree.f * tree.g)
    dg = sum(tree.f * tree.g * tree.d) / G
    hg = sum(tree.f * tree.g * tree.h) / G
    ag = sum(tree.f * tree.g * tree.a) / G
}

# Tree-level growth percentages for big trees (d>0)
model tree where d>0 {
    id5p = exp(5.4625 - 0.6675 * log(ag) - 0.4758 * log(G) + 0.1173 * log(dg) - 0.9442 * log(hdom) - 0.3631 * log(d) + 0.7762 * log(h)) where s=1
    id5p = exp(6.9342 - 0.8808 * log(ag) - 0.4982 * log(G) + 0.4159 * log(dg) - 0.3865 * log(hg) - 0.6267 * log(d) + 0.1287 * log(h))
    ih5p = exp(5.4636 - 0.9002 * log(ag) + 0.5475 * log(dg) - 1.1339 * log(h)) where s=1
    ih5p = 12.7402 - 1.1786 * log(ag) - 0.0937 * log(G) - 0.1434 * log(dg) - 0.8070 * log(hg) + 0.7563 * log(d) - 2.0522 * log(h)
}

# Tree-level growth for big trees (d>0)
model tree {
    id5 = d*((1+0.01*id5_p)^5 - 1)
    ih5 = h*((1+0.01*ih5_p)^5 - 1)
}

# Tree-level growth for small trees (d=0)
model tree {
    ih5 = 0.3
    id5 = 1 where h+ih5 >= 1.3
    id5 = 0
}
```

To simulate growth, we can compute the 5-year diameter and height increments `id5` and `ih5`.
Let's create an *m3 application*, `app.lua`:

```lua
-- read database file name from command line
local db = assert(..., "usage: app.lua database")
data.attach(db)

-- define the growth operation: replace every tree-level variable `x` with `x + ix5`,
-- if `ix5` is defined (i.e. d->d+id5, h->h+ih5)
local grow = data.transaction()
    :update("tree", function(name)
        if data.defined(string.format("i%s5", name)) then
            return string.format("%s + i%s5", name, name)
        end
    end)

-- define some outputs. we want to print the total basal area of the stand (G).
local get_G = data.transaction():read("G")
local function report_G() print("G:", get_G()) end

-- simulate 15 (3x5) years of growth
control.simulate = control.all { grow, report_G, grow, report_G, grow, report_G }
```

You can run the app via:
```
m3 app.lua <database>
```
where `<database>` is an SQLite database.

## Building

You'll need:
* A C compiler
* [Rust toolchain](https://rustup.rs/) for building fhk
* git

First, clone the *m3* repository with submodules:
```
git clone --recurse-submodules https://github.com/menu-hanke/m3
```

Switch into the `m3` directory and build dependencies:
```
cd m3
make deps
```

Now you can build *m3*:
```
make
```

## Creating simulators
(TODO)

## Embedding m3
(TODO)