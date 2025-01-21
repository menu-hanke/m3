local hooks = { [0]=newproxy(true) }
getmetatable(hooks[0]).__gc = function() for i=#hooks, 1, -1 do hooks[i]() end end

-- usage:
--   shutdown([obj,] finalizer)
--   shutdown(obj, "anchor")
return function(x,f)
	local hook
	if f == "anchor" then
		hooks[x] = true
	else
		if f then
			hook = function() f(x) end
		else
			hook = x
		end
		table.insert(hooks, hook)
	end
	return x
end
