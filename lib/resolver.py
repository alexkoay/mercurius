from .action import Action
import logging

log = logging.getLogger('base')

def get_registry():
    if get_registry.memo is None:
        get_registry.memo = { }
        found = Action.__subclasses__()
        while len(found) > 0:
            cls = found.pop(0)

            if cls._id:
                if cls._id in get_registry.memo:
                    log.warning('duplicate id found: %s (%s, %s)', cls._id, get_registry.memo[cls._id], cls)
                get_registry.memo[cls._id] = cls

            found.extend(cls.__subclasses__())

    return get_registry.memo
get_registry.memo = None

def execute(conn, actions, complete=False):
    registry = get_registry()
    log.log(15, 'Found %s actions.', len(registry))
    log.log(15, 'Actions: %s', ', '.join(registry.keys()))

    if '*' in actions: actions = registry.keys()
    todo, done = set(actions), set()

    cur = conn.cursor()
    cur.execute('SET CONSTRAINTS ALL DEFERRED')
    while len(todo) > 0:
        some = False
        for name in sorted(todo):
            if name not in registry: continue

            action = registry[name]
            if done >= action._before:
                some = True

                act = action(cur, complete=complete)
                act.run()

                todo.remove(name)
                done.add(name)
                todo.update(action._after)
            elif len(action._before) > 0:
                some = True
                todo.update(action._before)
                todo -= done

        if not some:
            log.error('Could not resolve: (%s)', ', '.join(sorted(todo)))
            break