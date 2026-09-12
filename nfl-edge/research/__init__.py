"""Top-level research workspace package.

NOTE: this is distinct from ``model.research``. Some test modules put
``nfl-edge/model`` on ``sys.path``, which lets ``model/research`` shadow this
package under the bare ``research`` name. Importers of this package must make
sure ``nfl-edge`` precedes ``nfl-edge/model`` on ``sys.path`` (and evict a
previously cached top-level ``research`` module) before importing.
"""
