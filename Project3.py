
import pymongo
import argparse
import pandas as pd
import re
import subprocess
import shlex
import os
from frameioclient import FrameioClient

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]

mycol1 = mydb["Collection1"]
mycol2 = mydb["Collection2"]

def OpenFile(filename) :
    file = open(filename, 'r')
    content = file.read()
    file.close()
    return content

def StripXYtech(line):
    split = line.split('/')
    XyLocations = "/" + split[1] + "/" + split[2]
    return line.replace(XyLocations, "")

def StripBL(line):
    split = line.split('/')
    BlLocations = "/" + split[1] + "/" 
    return line.replace(BlLocations, "")


def format_range(frame_range):
    if len(frame_range) == 1:
        return str(frame_range[0])
    else:
        return f"{frame_range[0]}-{frame_range[-1]}"

def FrameRanges(frame_numbers) :
    ranges = []
    start_frame = frame_numbers[0]
    current_range = [start_frame]

    for frame in frame_numbers[1:]:
        if frame == current_range[-1] + 1:
            current_range.append(frame)
        else:
            ranges.append(current_range)
            current_range = [frame]

    ranges.append(current_range)
    formatted_ranges = [format_range(rng) for rng in ranges]
    return formatted_ranges

def ComputeBLFrames(parsed_BLData):
    data = []
    for line in parsed_BLData : 
        directory = line[0]
        frames = FrameRanges(line[1])
        for frame in frames :
            data.append(directory + " " + frame)
    return data
            
def ParseBaselight(BLData):
    baselight_locations = []
    for line in BLData.splitlines():
        # Ignore empty lines and lines containing <err> or <null>
        if not line :
            continue
        line = line.replace(" <err>", "")
        line = line.replace(" <null>", "")
        parts = line.split()
        location = parts[0].replace("/baselightfilesystem1", "")
        frame_numbers = [int(frame) for frame in parts[1:] if frame.isdigit()]
        baselight_locations.append((location, frame_numbers))
    return baselight_locations
       
def ComputeXytechLocations(XYData) :
    paths = []
    block = False
    for line in XYData.splitlines():
        if block :
            if line == "" :
                block = False
            else :
                paths.append(line)
        if "Location:" in line :
            block = True
    return paths

def Xytech(XYData) :
    Producer = ""
    WorkOrder = ""
    Operator = ""
    Job = ""
    Notes = ""
    data = []
    block = False
    for line in XYData.splitlines() :
        if "Workorder" in line :
            split = line.split()
            WorkOrder = split[2]
        if "Producer" in line :
            split = line.split()
            Producer = split[1] + " " + split[2]
        if "Operator" in line :
            split = line.split()
            Operator = split[1] + " " + split[2]
        if "Job" in line :
            split = line.split()
            Job = split[1]
        if block :
            if line == "" :
                block = False
            else :
                Notes += line
        if "Notes:" in line :
            block = True
    data.append(Producer)
    data.append(WorkOrder)
    data.append(Operator)
    data.append(Job)
    data.append(Notes)
    return data  

def ExportToXLSX(csv_data, image_folder, filename):
    # Create a DataFrame from the processed data
    df = pd.DataFrame(csv_data, columns=['Producer', 'Operator:', 'Job:', 'Notes:'])
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    # Write the DataFrame to the Excel file
    df.to_excel(writer, index=False)
    worksheet = writer.sheets['Sheet1']
    
    # Insert images
    row = 4
    col = 3

    for file in os.listdir(image_folder):
        filename = os.fsdecode(file)
        if filename.endswith(".jpg"):
            worksheet.insert_image(row, col, os.path.join(image_folder, filename))
            row += 1
    
    writer._save()

    
def PopulateDBBaselight(BLLocation_frames):
    list = []
    for entry in BLLocation_frames:
            split_entry = entry.split()
            location = split_entry[0]
            frames = split_entry[1]
            document = {
                'Folder': location,
                'Frames': frames
            }
            list.append(document)

    if list:    
        mycol1.insert_many(list)

def PopulateDBXytech(xytech_locations, XYTechData):
    list = []
    for entry in xytech_locations:
        document = {
            'Workorder': XYTechData[1],
            'Location': entry
        }
        list.append(document)

    if list:    
        mycol2.insert_many(list)

def ProcessTimecode(frame):
    seconds = 0
    minutes = 0
    hours = 0
    seconds = int(frame) // 60
    rframes = int(frame) % 60
    if seconds >= 60:
        minutes = seconds // 60
        seconds = seconds % 60
        if minutes >= 60:
            hours = minutes // 60
            minutes = minutes % 60
    return(f"{hours:02}:{minutes:02}:{seconds:02}:{rframes:02}")

def ProcessVideo(file_name):
    command = "ffprobe -v error -select_streams v:0  -show_entries stream=nb_frames -print_format default=nokey=1:noprint_wrappers=1 " + file_name
    frame_amount = subprocess.run(command, shell=True, capture_output=True, text=True)
    total_frames = int(frame_amount.stdout.strip())

    pattern = re.compile(r'\d+-\d+')
    pointers1 = mycol1.find({'Frames': {'$regex': pattern}})
    time_codes = []
    for document1 in pointers1:
        frame_range = re.search(pattern, document1['Frames']).group()
        start_frame, end_frame = map(int, frame_range.split('-'))
        start_seconds = start_frame/60
        end_seconds = end_frame/60
        middle_frame = (start_frame + end_frame) // 2
        start_time = ProcessTimecode(start_frame)
        end_time = ProcessTimecode(end_frame)
        formatted_frames = f"{start_time}-{end_time}"
        pointers2 = mycol2.find({}, {'Location': 1, '_id': 0})
        for document2 in pointers2:
            if StripXYtech(document2['Location']) == document1['Folder']:
                if end_frame < total_frames:
                    thumbnail_name = f"{middle_frame}.jpg"
                    middle_seconds = middle_frame / 60
                    thumbnail_command = f"ffmpeg -ss {middle_seconds} -i {file_name} -vf scale=96:74 -vframes 1 {thumbnail_name} -loglevel 0"
                    process = subprocess.Popen(thumbnail_command.split(), stdout=subprocess.PIPE)
                    process.wait()
                    clip_name = f"{start_seconds}-{end_seconds}.mp4"
                    command = f"ffmpeg -ss {start_seconds} -to {end_seconds} -i {file_name} -vcodec copy -async 1 {clip_name} -loglevel 0"
                    clip_process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE)
                    clip_process.wait()
                    for file in os.listdir("C:/Users/Dawson/Documents/COMP_467_CHAJA/Project3"):
                        filename = os.fsdecode(file)
                        if filename.endswith(".mp4"):
                            FrameIO(file)
                    row = [(document2['Location']), frame_range, formatted_frames]
                    time_codes.append(row)
    return time_codes
    
def FrameIO(file_name):
    client = FrameioClient("fio-u-zZc-gpl8LPNyxJ758r2GCGhFpmb-xHKpIbaZbKCoqHOxO5rbn9ZZOARzk6pGzKTr")
    client.assets.upload("0190b2d4-00a1-42ea-aae3-dd56e690cf34", file_name)

# Read data from files
BaselightData = OpenFile("Baselight_export.txt")
XYData = OpenFile("Xytech.txt")

# Process Baselight data
parsed_BLData = ParseBaselight(BaselightData)
BLLocation_frames = ComputeBLFrames(parsed_BLData)      

# Process Xytech data
XYTechData = Xytech(XYData)
xytech_locations = ComputeXytechLocations(XYData)  
    


parser = argparse.ArgumentParser(description='Process CSV/Excel file and insert into MongoDB.')
parser.add_argument('--file', type=str, help='File to process')
parser.add_argument('--baselight', action='store_true', help='Insert into Collection1')
parser.add_argument('--xytech', action='store_true', help='Insert into Collection2')
parser.add_argument('--process', action='store_true', help='Get DB Answers')
parser.add_argument('--output', action='store_true', help='export CSV')

args = parser.parse_args()

if args.baselight:
    PopulateDBBaselight(BLLocation_frames)
if args.xytech:
    PopulateDBXytech(xytech_locations, XYTechData)
if args.process:
    ProcessVideo(args.file)
if args.output:
    video_data = ProcessVideo(args.file)
    csv_data = []
    row = [XYTechData[0], XYTechData[2], XYTechData[3], XYTechData[4]]
    row2 = []
    row3 = ['Locations:', 'Frames to Fix:', 'TimeCode:', 'Thumbnail']
    csv_data.append(row)
    csv_data.append(row2)
    csv_data.append(row3)
    for entry in video_data:
        csv_data.append(entry)
   
    ExportToXLSX(csv_data, "C:/Users/Dawson/Documents/COMP_467_CHAJA/Project3", 'output.xlsx')

