"""SynodSQL: a git-style replicated SQL database for a small group of trusted servers.

A ``Database`` is one node's store. Open it, write and query it, sync it with
other nodes by files (``seal``, ``export``, ``merge``), or run it as a cluster
node in the background (``serve``) while the program goes on using it.
"""

from ._native import Database, Error, __version__, make_certs, remote, row_id_str

__all__ = ["Database", "Error", "__version__", "make_certs", "remote", "row_id_str"]
