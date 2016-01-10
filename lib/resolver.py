from .action import registry
import logging

def execute(conn, actions, complete=False):
    log = logging.getLogger('base')
    log.info('Found %s actions.', len(registry))
    log.info('Actions: %s', ', '.join(registry.keys()))

    if '*' in actions: actions = registry.keys()
    todo, done = set(actions), set()

    cur = conn.cursor()
    cur.execute('SET CONSTRAINTS ALL DEFERRED')
    log.getChild('sql').info('> %s', cur.query.decode('utf-8'))
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