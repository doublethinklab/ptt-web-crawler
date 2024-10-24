PATH=/root/ptt-web-crawler
cd $PATH
source venv/bin/activate && python main.py
deactivate
cd -

# Run at 12:00, 14:00, 18:00, 21:00, 23:00, 3:00, 8:00 UTC+8 every day
# 0 0,4,6,10,13,15,19 * * * /root/ptt-web-crawler/run.sh