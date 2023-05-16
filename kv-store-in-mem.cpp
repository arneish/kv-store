// To compile and run:
// g++ -std=c++17 -pthread kv-store-in-mem.cpp -o kv && ./kv

#include<iostream>
#include<fstream>
#include<thread>
#include<mutex>
#include<unordered_map>

using namespace std;

vector<string> getTokens(string& str){
    vector<string> tokens;
    size_t pos;
    while((pos=str.find(" "))!=string::npos){
        tokens.emplace_back(str.substr(0, pos));
        str=str.substr(pos+1);
    }

    tokens.emplace_back(str);

    return tokens;
}

enum EventType{
    Update = 0,
    Delete = 1,
};

struct Record{
    EventType eventType;
    string key;
    string val;

    Record(EventType type, string key, string val):eventType(type),key(key),val(val){}

    Record(EventType type, string key):eventType(type),key(key),val(""){}
};

class KvStore{    
    public:
        void Put(string key, string value){
            this->appendUpdateToLogFile(false, key, value);
            this->inMemMap[key]=value;
        }

        string Get(string key){
            if(!this->inMemMap.count(key)) return "";
            return this->inMemMap[key];
        }

        void Delete(string key){
            this->appendUpdateToLogFile(true, key);
            this->inMemMap.erase(key);
        }

        void PrintLogFile();

        void PrintInMemMap();

        KvStore(){
            // load inMemMap with the current state of the Database.
            this->loadInMemMapFromDisk();

            // scheduled background thread that will keep flushing 
            // new log file entries to DB with the most recent {key,value} pairs.
            this->scheduledLogFilePoller = thread([this] {this->flushLogToDataFiles();});
        }

        ~KvStore(){
            terminateScheduledLogFilePoller = true;
            this->scheduledLogFilePoller.join();
            cout<<"Destructor completing\n";
        }
    
    private:
        void loadInMemMapFromDisk();

        void appendUpdateToLogFile(bool isDelete, string key, string val="");
        
        void flushLogToDataFiles();

        int getLargestIndexInLogFileFromDisk(){
            return this->lastRecordIndexInLogFile;
        }
        
        int getLastIndexFlushedFromDisk(){
            return this->lastIndexFlushed;
        }

        void setLastIndexFlushed(int lastIndexInLogFile){
            this->lastIndexFlushed=lastIndexInLogFile;
        }

        unordered_map<string, string> loadExistingKvStoreFromDisk(){
            unordered_map<string,string> result;
            ifstream diskFile("disk.txt");
            string currLine;
            while(getline(diskFile,currLine)){
                auto tokens=getTokens(currLine);
                result[tokens[0]]=tokens[1];
            }

            diskFile.close();

            return result;
        }

        vector<Record> getAllLogEventsWithIndexBetween(int index1, int index2){
            vector<Record> result;
            ifstream LogFile(this->logFileName);
            int index;
            string currLine, type, key, value;
            while(getline(LogFile,currLine)){
                vector<string> tokens=getTokens(currLine);
                if(stoi(tokens[0])<=index1 || stoi(tokens[0])>index2){continue;}
                if(tokens[1]=="PUT"){
                    result.emplace_back(Record(EventType::Update, tokens[2], tokens[3]));
                }
                else{ 
                    // DELETE
                    result.emplace_back(Record(EventType::Delete, tokens[2]));
                }
            }

            LogFile.close();
            cout<<"Record count with indices between:"<<index1<<","<<index2<<" :"<<result.size()<<endl;
            return result;
        }

        void flushMapToDisk(unordered_map<string, string>&map){
            ofstream diskFile("disk.txt",ios_base::out);
            for(const auto&[k,v]:map){
                diskFile<<k<<" "<<v<<"\n";
            }

            diskFile.close();
        }

        
        thread scheduledLogFilePoller;
        bool terminateScheduledLogFilePoller = false;
        unordered_map<string, string> inMemMap;
        const string logFileName = "logFile.txt";
        int lastIndexFlushed = -1;
        int lastRecordIndexInLogFile = -1;
};

void KvStore::appendUpdateToLogFile(bool isDelete, string key, string val){
    ofstream logFile;
    logFile.open(this->logFileName,ios_base::app);
    lastRecordIndexInLogFile++;
    if(isDelete){
        logFile<<lastRecordIndexInLogFile<<" DELETE "<<key<<"\n";
    }
    else{
        logFile<<lastRecordIndexInLogFile<<" PUT "<<key<<" "<<val<<"\n";
    }

    logFile.close();
}

 void KvStore::flushLogToDataFiles(){
    while(!this->terminateScheduledLogFilePoller){
        this_thread::sleep_for(chrono::seconds(10));

        int lastIndexInLogFile = this->getLargestIndexInLogFileFromDisk();
        int lastIndexFlushed = this->getLastIndexFlushedFromDisk();
        if(lastIndexInLogFile<=lastIndexFlushed){
            cout<<"No new records to write. lastIndexInLogFile:"<<lastIndexInLogFile<<", LastIndexFlushed:"<<lastIndexFlushed<<endl;
            continue;
        }

        // flush last batch of records from logFile to dataFiles.
        vector<Record> recordsToWrite = this->getAllLogEventsWithIndexBetween(lastIndexFlushed, lastIndexInLogFile); //(lastIndexFlushed,lastIndexInLogFile]
        cout<<"Records To Write:"<<recordsToWrite.size()<<endl;
        
        // consolidate all the pending upgrades from the logFile onto a final state "desiredState" map, for the AFFECTED keys.
        unordered_map<string, string> desiredState;
        for(auto record: recordsToWrite){
            if(record.eventType==EventType::Delete){
                desiredState[record.key]="DELETED"; // reserved keyword for the case of deletion.
            }
            else{
                desiredState[record.key]=record.val;
            }
        }

        // load the existing map into memory from the disk. (In the real world, we will only load the specific disk "segments"/"blocks" containing the keys that need to be updated)
        unordered_map<string,string> existingState = this->loadExistingKvStoreFromDisk();
        
        // apply the desired changes
        for(const auto&[key,val]: desiredState){
            if(val=="DELETED"){
                existingState.erase(key);
            }
            else{
                existingState[key]=val;
            }
        }

        // flush the modified map back to the disk;
        this->flushMapToDisk(existingState);

        // update the checkpoint
        this->setLastIndexFlushed(lastIndexInLogFile);
    }

    cout<<"Exited loop\n";
}

void KvStore::loadInMemMapFromDisk(){
    ifstream diskFile("disk.txt");
    string currLine, key, val;
    while(getline(diskFile,currLine)){
        size_t pos = 0;
        string key;
        while ((pos = currLine.find(" ")) != string::npos) {
            key = currLine.substr(0, pos);
            currLine.erase(0, pos + 1);
        }
        string value = currLine;

        this->inMemMap[key]=value;
    }

    diskFile.close();
}

void KvStore::PrintLogFile(){
    string line;
    ifstream logFile(this->logFileName);
    while(getline(logFile,line)){
        cout<<line<<"\n";
    }
    logFile.close();
}

void KvStore::PrintInMemMap(){
    for(auto&[k,v]:this->inMemMap){
        cout<<k<<","<<v<<endl;
    }
}


int main(){
    ofstream DiskFile("disk.txt", ios_base::out);
    DiskFile << "7 " <<"arneish7\n"; 
    DiskFile << "8 " <<"arneish8\n";
    DiskFile.close();

    ofstream LogFile("logFile.txt", ios_base::out);
    LogFile.close();

    KvStore kv;
    kv.Put("1","arneish");
    kv.Put("2","prateek");
    kv.Put("3","hello");
    kv.Put("4","world");
    kv.Put("5","databricks");
    kv.Delete("1");
    kv.Put("2", "prateek2");

    this_thread::sleep_for(chrono::seconds(15));

    kv.Put("5","databricksSQL");
    kv.Delete("4");
    kv.Put("10", "Charlotte");

    while(true){
        int a=1;
    }

    return 0;
}