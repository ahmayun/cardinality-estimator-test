package fuzzer.data.tables

case class TableMetadata(
                          identifier: String,                    // Table name or ID
                          columns: Seq[ColumnMetadata],
                          metadata: Map[String, String] = Map.empty // Extra info (e.g. source, owner)
                        ) {
  def columnNames: Seq[String] = columns.map(_.name)

  def keyColumns: Seq[ColumnMetadata] = columns.filter(_.isKey)

  def nonKeyColumns: Seq[ColumnMetadata] = columns.filterNot(_.isKey)

}