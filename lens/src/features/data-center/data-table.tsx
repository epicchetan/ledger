import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type Row,
  type SortingState,
} from "@tanstack/react-table"
import type { ReactNode } from "react"
import { Fragment } from "react"
import { useState } from "react"

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { cn } from "@/lib/utils"

interface DataTableProps<TData, TValue> {
  columns: ColumnDef<TData, TValue>[]
  data: TData[]
  emptyMessage: string
  initialSorting?: SortingState
  getRowId?: (row: TData, index: number) => string
  getRowClassName?: (row: Row<TData>) => string
  onRowClick?: (row: Row<TData>) => void
  renderExpandedRow?: (row: Row<TData>, colSpan: number) => ReactNode
  maxHeightClassName?: string
  minWidthClassName?: string
}

export function DataTable<TData, TValue>({
  columns,
  data,
  emptyMessage,
  initialSorting = [],
  getRowId,
  getRowClassName,
  onRowClick,
  renderExpandedRow,
  maxHeightClassName = "max-h-[calc(100svh-12.5rem)]",
  minWidthClassName = "min-w-[1120px]",
}: DataTableProps<TData, TValue>) {
  const [sorting, setSorting] = useState<SortingState>(initialSorting)
  // TanStack Table intentionally returns table APIs that React Compiler cannot memoize.
  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data,
    columns,
    getRowId,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    onSortingChange: setSorting,
    state: { sorting },
  })

  const rows = table.getRowModel().rows
  const visibleColumnCount = table.getVisibleLeafColumns().length

  return (
    <div className={cn("thin-scrollbar min-h-0 overflow-auto", maxHeightClassName)}>
      <Table className={cn("border-collapse text-xs", minWidthClassName)}>
        <TableHeader className="sticky top-0 z-10 bg-card text-[0.68rem] uppercase text-muted-foreground">
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id} className="hover:bg-transparent">
              {headerGroup.headers.map((header) => (
                <TableHead key={header.id} className="h-auto border-b border-border px-3 py-2 font-medium text-muted-foreground">
                  {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                </TableHead>
              ))}
            </TableRow>
          ))}
        </TableHeader>
        <TableBody>
          {rows.length > 0 ? (
            rows.map((row) => (
              <Fragment key={row.id}>
                <TableRow
                  className={cn("border-border/70 hover:bg-muted/35", onRowClick ? "cursor-pointer" : undefined, getRowClassName?.(row))}
                  onClick={onRowClick ? () => onRowClick(row) : undefined}
                >
                  {row.getVisibleCells().map((cell) => (
                    <TableCell key={cell.id} className="px-3 py-2.5 align-middle">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
                {renderExpandedRow?.(row, visibleColumnCount)}
              </Fragment>
            ))
          ) : (
            <TableRow>
              <TableCell colSpan={visibleColumnCount} className="h-24 text-center text-xs text-muted-foreground">
                {emptyMessage}
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
  )
}
