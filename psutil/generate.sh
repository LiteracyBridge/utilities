rm new_recipients.csv
rm new_recipients_map.csv

echo MEDA
programspec/app.py reconcile --dropbox ~/tmp --spec MEDAspec2-new.xlsx --acm meda -u recip >generate_meda.txt
cp recipients.csv new_recipients.csv
cp recipients_map.csv new_recipients_map.csv
wc recipients.csv
wc recipients_map.csv
wc new_recipients.csv
wc new_recipients_map.csv

echo CARE
programspec/app.py reconcile --dropbox ~/tmp --spec CAREspec-new.xlsx --acm care -u recip >generate_care.txt
tail +2 recipients.csv >> new_recipients.csv
tail +2 recipients_map.csv >>new_recipients_map.csv
wc recipients.csv
wc recipients_map.csv
wc new_recipients.csv
wc new_recipients_map.csv

echo UNICEF-2
programspec/app.py reconcile --dropbox ~/tmp --spec UNICEF2spec-new.xlsx --acm unicef-2 -u recip >generate_unicef2.txt
tail +2 recipients.csv >> new_recipients.csv
tail +2 recipients_map.csv >>new_recipients_map.csv
wc recipients.csv
wc recipients_map.csv
wc new_recipients.csv
wc new_recipients_map.csv

