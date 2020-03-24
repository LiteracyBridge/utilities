# Bumps the last version number in a line like "  version='1.2.3'," => 1.2.4
# Passes other lines unchanged.
BEGIN     {FS = "['.\"]"
           rc = 1}
/version/ {print NF, $2, $3, $4 > "/dev/stderr";
           printf("    version='%d.%d.%d',\n",$2,$3,$4+1);
           rc = 0;
           next;
          }
          {print}
END       {exit rc}          
