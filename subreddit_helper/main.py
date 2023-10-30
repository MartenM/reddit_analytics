import argparse
from dotenv import load_dotenv
import praw
import os
import datetime
import pandas as pd
import prawcore as pc
import time
import logging

def log(msg: str):
    now = datetime.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(f"[{current_time}] {msg}")
    return

def getData(reddit, row, attempts = 10):
    if attempts == 0:
        raise Exception("Too many requests. Limits exceeded")

    try:
        sub = reddit.subreddit(row['subreddit'])
        return[
            row['subreddit'],
            sub.over18,
            sub.name,
            sub.subscribers,
            True
        ]
    except (pc.exceptions.Redirect, pc.exceptions.NotFound, pc.exceptions.Forbidden):
        # Subreddit is either not available or has been banned.
        return [
            row['subreddit'],
            None,
            None,
            None,
            False
        ]
    except pc.exceptions.TooManyRequests as e:
        log(f"TooManyRequests: Retrying in 1 minute" )
        time.sleep(60)
        return getData(reddit, row, attempts - 1)
    except KeyboardInterrupt:
        log('Termination received. Goodbye!')
        exit()

def main(args):
    log("Starting program...")
    log(f"Logging in as: {os.environ['REDDIT_USERNAME']} ({os.environ['CLIENT_ID']})")

    reddit = praw.Reddit(
        client_id=os.environ['CLIENT_ID'],
        client_secret=os.environ['CLIENT_SECRET'],
        password=os.environ['REDDIT_PASSWORD'],
        user_agent="social_network_bot:v0.0.1",
        username=os.environ['REDDIT_USERNAME'],
        ratelimit_seconds=999999999999999,
    )

    
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    for logger_name in ("praw", "prawcore"):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)

    print(reddit._core._requestor._http.headers)

    log(f"Logged in as: {reddit.user.me()}")

    subreddits = pd.read_csv(f"{args.input_file}.csv")
    if (args.skip_entries > 0):
        log(f"Skipping the first {args.skip_entries}")
        subreddits = subreddits.tail(subreddits.shape[0] - args.skip_entries)
        log(f"First entry to be processed: {subreddits.iloc[0]['subreddit']}")
        if (args.debug == True):
            exit()

    subreddits_count = subreddits.shape[0]

    output_data = []
    last_batch = time.time()
    for index, row in subreddits.iterrows():
        if (index % 10 == 0):
            delta_time = time.time() - last_batch
            last_batch = time.time()

            time_left = int(delta_time * (subreddits_count - (index - args.skip_entries)) / 60)

            log(f"Processing... [{index + 1 - args.skip_entries}/{subreddits_count}] +{delta_time}  (Time left: {time_left} minutes)")
        
        if (index % args.split_size == 0 and len(output_data) != 0):
            log("Saving intermediate result...")

            # Convert to frame and save to CSV
            if os.path.isfile(f"{args.output_file}-{index - len(output_data)}-{index}.csv"):
                log("FILE ALREADY EXISTS, EXITING")
                exit()
            
            output_frame = pd.DataFrame(output_data, columns=['subreddit', 'nsfw', 'name', 'subscribers', 'available'])
            output_frame.to_csv(f"{args.output_file}-{index - len(output_data)}-{index}.csv")
            output_data.clear()

        if (args.max_fetch != 0 and index >= args.max_fetch + args.skip_entries):
            log(f"Breaking due to max fetch exceeded ({args.max_fetch})")
            break
        
        output_data.append(getData(reddit, row))
        
    
    log("Saving last result...")
    # Convert to frame and save to CSV
    output_frame = pd.DataFrame(output_data, columns=['subreddit', 'nsfw', 'name', 'subscribers', 'available'])
    output_frame.to_csv(f"{args.output_file}-{index - len(output_data)}-{index}.csv")

    log("Done! Have a nice day!")
    pass

if __name__ == "__main__":
    # Load ENV variables
    load_dotenv()

    parser = argparse.ArgumentParser(
                    prog='Reddit Sub-reddit helper',
                    description='Converts CSV of subreddits to CSV of subreddits including specific metadata.',
                    epilog='Created by: Marten Struijk')
    
    parser.add_argument('-i', '--input', default="../data/subreddits", help="The input file", dest="input_file")
    parser.add_argument('-o', '--output', default="../data/subreddits-meta", help="The output file. Will have .csv appended.", dest="output_file")
    parser.add_argument('-m', '--max', type=int, default=0, help="Max amount of subreddits to be fetched. Useful for debugging", dest="max_fetch")
    parser.add_argument('-s', '--skip', type=int, default=0, help="Skip the initial x subreddits", dest="skip_entries")
    parser.add_argument('--debug', type=bool, default=False, help="Debug mode", dest="debug")
    parser.add_argument('--split', type=int, default=1000, help="Subreddits in one saved batch", dest="split_size")

    main(parser.parse_args())