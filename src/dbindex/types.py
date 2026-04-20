from typing import TypeAlias

Primitive: TypeAlias = (
    bool | int | float | str | bytes | None
)

Row: TypeAlias = dict[str, Primitive]
