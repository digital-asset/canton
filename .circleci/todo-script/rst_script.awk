# Start matching when a todo block opens
/todo::/ {matching = 1}

# Stop matching on an empty line
/^\s*$/ {if(matching) printf("\n"); matching = 0}

# When matching, print the intermediate line
matching {
    printf("%s <SEP> ", $0)
}
