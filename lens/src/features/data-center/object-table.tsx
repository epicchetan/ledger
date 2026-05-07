import type { Column, ColumnDef } from "@tanstack/react-table"
import { ArrowUpDown, Cloud, HardDrive, MoreVertical, Trash2 } from "lucide-react"
import { useMemo } from "react"

import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card"
import { DataTable } from "@/features/data-center/data-table"
import type { StoreObject } from "@/features/data-center/types"

interface ObjectTableProps {
  objects: StoreObject[]
  onDelete: (object: StoreObject) => void
}

export function ObjectTable({ objects, onDelete }: ObjectTableProps) {
  const columns = useMemo<ColumnDef<StoreObject>[]>(
    () => [
      {
        id: "role",
        accessorFn: (object) => object.role,
        header: ({ column }) => <SortableHeader column={column} label="Role" />,
        cell: ({ row }) => (
          <div className="w-20">
            <RoleBadge role={row.original.role} />
          </div>
        ),
      },
      {
        id: "kind",
        accessorFn: (object) => object.kind,
        header: ({ column }) => <SortableHeader column={column} label="Kind" />,
        cell: ({ row }) => (
          <div className="max-w-56 truncate font-medium">
            {row.original.kind}
          </div>
        ),
      },
      {
        id: "fileName",
        accessorFn: (object) => object.fileName,
        header: ({ column }) => <SortableHeader column={column} label="File" />,
        cell: ({ row }) => <ObjectHover object={row.original} />,
      },
      {
        id: "size",
        accessorFn: (object) => object.sizeBytes,
        header: ({ column }) => <SortableHeader column={column} label="Size" />,
        cell: ({ row }) => (
          <div className="w-24 text-muted-foreground tabular-nums">
            {row.original.size}
          </div>
        ),
      },
      {
        id: "local",
        accessorFn: (object) => (object.local ? 1 : 0),
        header: ({ column }) => (
          <SortableHeader column={column} label="Local" />
        ),
        cell: ({ row }) => <LocalBadge object={row.original} />,
      },
      {
        id: "remote",
        accessorFn: (object) => object.remote?.key ?? "",
        header: ({ column }) => (
          <SortableHeader column={column} label="Remote" />
        ),
        cell: ({ row }) => <RemoteBadge object={row.original} />,
      },
      {
        id: "updated",
        accessorFn: (object) => object.updatedAt,
        header: ({ column }) => (
          <SortableHeader column={column} label="Updated" />
        ),
        cell: ({ row }) => (
          <div className="w-28 text-muted-foreground">
            {row.original.updatedAt}
          </div>
        ),
      },
      {
        id: "actions",
        header: "",
        enableSorting: false,
        cell: ({ row }) => (
          <ObjectActions object={row.original} onDelete={onDelete} />
        ),
      },
    ],
    [onDelete]
  )

  return (
    <div className="min-h-0 overflow-hidden border border-border bg-card/30">
      <DataTable
        columns={columns}
        data={objects}
        emptyMessage="No store objects found."
        getRowId={(object) => object.id}
        initialSorting={[{ id: "updated", desc: true }]}
        minWidthClassName="min-w-[980px]"
      />
    </div>
  )
}

function ObjectActions({
  object,
  onDelete,
}: {
  object: StoreObject
  onDelete: (object: StoreObject) => void
}) {
  return (
    <div className="text-right">
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            aria-label={`Actions for ${object.fileName}`}
          >
            <MoreVertical className="size-4" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="rounded-none">
          <DropdownMenuItem
            className="text-destructive focus:bg-destructive/10 focus:text-destructive"
            onSelect={() => onDelete(object)}
          >
            <Trash2 className="size-3.5" />
            Delete
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  )
}

function SortableHeader<TData, TValue>({
  column,
  label,
}: {
  column: Column<TData, TValue>
  label: string
}) {
  return (
    <button
      type="button"
      className="-ml-1 inline-flex h-6 items-center gap-1 px-1 text-[0.68rem] text-muted-foreground uppercase transition-colors hover:text-foreground"
      onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
    >
      {label}
      <ArrowUpDown className="size-3" />
    </button>
  )
}

function RoleBadge({ role }: { role: string }) {
  const tone =
    role === "raw"
      ? "border-sky-600/25 bg-sky-500/10 text-sky-700 dark:border-sky-400/25 dark:bg-sky-400/10 dark:text-sky-200"
      : "border-cyan-600/25 bg-cyan-500/10 text-cyan-700 dark:border-cyan-400/25 dark:bg-cyan-400/10 dark:text-cyan-200"

  return (
    <span
      className={`inline-flex border px-1.5 py-0.5 text-[0.68rem] font-medium ${tone}`}
    >
      {role}
    </span>
  )
}

function LocalBadge({ object }: { object: StoreObject }) {
  const hasLocal = object.local !== null
  return (
    <HoverCard>
      <HoverCardTrigger asChild>
        <button
          type="button"
          className="inline-flex w-24 items-center gap-1 text-left text-muted-foreground"
        >
          <HardDrive
            className={
              hasLocal
                ? "size-3 text-emerald-600 dark:text-emerald-300"
                : "size-3"
            }
          />
          <span>{hasLocal ? "local" : "remote only"}</span>
        </button>
      </HoverCardTrigger>
      <HoverCardContent
        align="start"
        className="w-80 border border-border bg-popover p-3 text-xs"
      >
        <DetailTitle title="Local" />
        <DetailRow
          label="Status"
          value={hasLocal ? "Stored locally" : "Remote only"}
        />
        <DetailRow
          label="Path"
          value={object.local?.relativePath ?? "-"}
          wrap
        />
        <DetailRow
          label="Used"
          value={object.local?.lastAccessedAt ?? "-"}
          wrap
        />
      </HoverCardContent>
    </HoverCard>
  )
}

function RemoteBadge({ object }: { object: StoreObject }) {
  return (
    <HoverCard>
      <HoverCardTrigger asChild>
        <button
          type="button"
          className="inline-flex max-w-[24rem] items-center gap-1 text-left text-muted-foreground"
        >
          <Cloud
            className={
              object.remote
                ? "size-3 text-emerald-600 dark:text-emerald-300"
                : "size-3"
            }
          />
          <span className="truncate">
            {remoteLabel(object.remote?.key ?? null)}
          </span>
        </button>
      </HoverCardTrigger>
      <HoverCardContent
        align="start"
        className="w-96 border border-border bg-popover p-3 text-xs"
      >
        <DetailTitle title="Remote" />
        <DetailRow label="Bucket" value={object.remote?.bucket ?? "-"} />
        <DetailRow label="Key" value={object.remote?.key ?? "-"} wrap />
        <DetailRow label="SHA" value={object.remote?.sha256 ?? "-"} wrap />
      </HoverCardContent>
    </HoverCard>
  )
}

function ObjectHover({ object }: { object: StoreObject }) {
  return (
    <HoverCard>
      <HoverCardTrigger asChild>
        <button
          type="button"
          className="max-w-[26rem] truncate text-left text-foreground"
        >
          {object.fileName}
        </button>
      </HoverCardTrigger>
      <HoverCardContent
        align="start"
        className="w-96 border border-border bg-popover p-3 text-xs"
      >
        <DetailTitle title={object.fileName} />
        <DetailRow label="Object ID" value={object.id} wrap />
        <DetailRow label="SHA" value={object.contentSha256} wrap />
        <DetailRow label="Role" value={object.role} />
        <DetailRow label="Kind" value={object.kind} wrap />
        <DetailRow label="Format" value={object.format ?? "-"} />
        <DetailRow label="Media" value={object.mediaType ?? "-"} wrap />
        <DetailRow label="Size" value={object.size} />
        <DetailRow label="Remote" value={object.remote?.key ?? "-"} wrap />
        <DetailRow
          label="Local"
          value={object.local?.relativePath ?? "-"}
          wrap
        />
        <DetailRow label="Created" value={object.createdAt} wrap />
      </HoverCardContent>
    </HoverCard>
  )
}

function remoteLabel(key: string | null) {
  if (!key) return "-"
  const parts = key.split("/")
  if (parts.length <= 3) return key
  return `${parts.at(-3)}/${parts.at(-2)}/${parts.at(-1)}`
}

function DetailTitle({ title }: { title: string }) {
  return <div className="mb-2 font-semibold text-foreground">{title}</div>
}

function DetailRow({
  label,
  value,
  wrap = false,
}: {
  label: string
  value: string
  wrap?: boolean
}) {
  return (
    <div className="grid grid-cols-[5.5rem_minmax(0,1fr)] gap-2 py-0.5">
      <div className="text-muted-foreground">{label}</div>
      <div
        className={
          wrap ? "break-all text-foreground" : "truncate text-foreground"
        }
      >
        {value}
      </div>
    </div>
  )
}
