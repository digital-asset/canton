-- Rename the offset store table to preserve lowercase casing
ALTER TABLE PEKKO_PROJECTION_OFFSET_STORE
    RENAME TO "pekko_projection_offset_store";

ALTER TABLE "pekko_projection_offset_store" ALTER COLUMN PROJECTION_NAME RENAME TO "projection_name";
ALTER TABLE "pekko_projection_offset_store" ALTER COLUMN PROJECTION_KEY RENAME TO "projection_key";
ALTER TABLE "pekko_projection_offset_store" ALTER COLUMN CURRENT_OFFSET RENAME TO "current_offset";
ALTER TABLE "pekko_projection_offset_store" ALTER COLUMN MANIFEST RENAME TO "manifest";
ALTER TABLE "pekko_projection_offset_store" ALTER COLUMN MERGEABLE RENAME TO "mergeable";
ALTER TABLE "pekko_projection_offset_store" ALTER COLUMN LAST_UPDATED RENAME TO "last_updated";

-- Rename the management table to preserve lowercase casing
ALTER TABLE PEKKO_PROJECTION_MANAGEMENT
    RENAME TO "pekko_projection_management";

ALTER TABLE "pekko_projection_management" ALTER COLUMN PROJECTION_NAME RENAME TO "projection_name";
ALTER TABLE "pekko_projection_management" ALTER COLUMN PROJECTION_KEY RENAME TO "projection_key";
ALTER TABLE "pekko_projection_management" ALTER COLUMN PAUSED RENAME TO "paused";
ALTER TABLE "pekko_projection_management" ALTER COLUMN LAST_UPDATED RENAME TO "last_updated";

-- Rename the index to exact lowercase
ALTER INDEX PROJECTION_NAME_INDEX
    RENAME TO "projection_name_index";
