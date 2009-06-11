{application, mochiconntest,
 [{description, "mochiconntest"},
  {vsn, "0.01"},
  {modules, [
    mochiconntest,
    mochiconntest_app,
    mochiconntest_sup,
    mochiconntest_web,
    mochiconntest_deps
  ]},
  {registered, []},
  {mod, {mochiconntest_app, []}},
  {env, []},
  {applications, [kernel, stdlib, crypto]}]}.
