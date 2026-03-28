"""Gateway logging package.

Keep package initialization minimal so importing one logging submodule does not
eagerly import the others and accidentally pull in storage modules too early.
"""

__all__: list[str] = []
