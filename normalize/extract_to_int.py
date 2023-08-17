from polars import pl
from .normalize_core import Table
def extract_hashable_to_int(table: Table):
  current_frame table.frame
  new_tables = []
  for col, dtype in table.frame.schema.items():
    if isinstance(dtype, pl.INTEGER_DTYPES):
      continue
    #if dtype.is_not_hashable()
    unique_rel = Relationship(
      f"{col}_{dtype}_INTEGER_INDEX",
      is_foreign_key= False)
    .frame
    .select(col)
    .unique()
    .with_row_count(
    unique_rel.on
    )
    current_frame = table.frame.join(unique_frame,
                       on=col,
                       how='inner')
    .drop(col)
    
    new_tables.append(
      Table(unique_frame,
            unique_rel))
    table = Table(current_frame,
                  table.relationships
                  +[unique_rel.make_opposite()])
  return table, new_tables
    
    
    
    
    
    
      
