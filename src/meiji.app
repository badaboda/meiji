{application, meiji,
 [{description, "meiji"},
  {vsn, "0.01"},
  {modules, [
    meiji,
    meiji_app,
    meiji_sup,
    meiji_web,
    meiji_deps
  ]},
  {registered, []},
  {mod, {meiji_app, []}},
  {env, []},
  {applications, [kernel, stdlib, crypto]}]}.
