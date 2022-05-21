#include <iostream>
#include <vector>
#include <pthread.h>
#include <unistd.h>
#include <ctime>
#include "hw2_output.h"
using namespace std;

pthread_mutex_t check = PTHREAD_MUTEX_INITIALIZER;
vector< vector< pair<int, pthread_mutex_t*> > > grid;

class ProperPrivate {
public:
    int gid;
    int rowSpan;
    int colSpan;
    struct timespec gatherTime;
    int numAreas;
    vector<pair<int, int>*> areas;
    ProperPrivate(int gid, int rowSpan, int colSpan, long gatherTime, int numAreas)
    :   gid(gid),
        rowSpan(rowSpan),
        colSpan(colSpan),
        numAreas(numAreas) 
    {
        this->gatherTime.tv_sec = 0;
        this->gatherTime.tv_nsec = (gatherTime % 1000) * 1000000;
    }

    ~ProperPrivate() {
        for (int i = 0; i < areas.size(); i++) {
            delete areas[i];
        }
    }

    void addArea(int row, int col) {
        pair<int, int>* newStart = new pair<int, int>(row, col);
        areas.push_back(newStart);
    }

    void pickUpCigbutts(pair<int, int>& area) {
        for (int i = area.first; i < area.first + rowSpan; i++) {
            for (int j = area.second; j < area.second + colSpan; j++) {
                while (grid[i][j].first) {   
                    nanosleep(&gatherTime, nullptr);
                    grid[i][j].first--;
                    hw2_notify(PROPER_PRIVATE_GATHERED, gid, i, j);
                }
                pthread_mutex_unlock(grid[i][j].second);
            }
        }
        hw2_notify(PROPER_PRIVATE_CLEARED, gid, 0, 0);
    }

    void checkForTheArea(pair<int, int>& area) {
        bool end = false;
        pair<int, int> lastFailedCoordinates(-1, -1);
        pthread_mutex_t* lastFailedCheck = nullptr;
        do {
            if (lastFailedCoordinates.first != -1 && lastFailedCoordinates.second != -1) {
                pthread_mutex_lock(lastFailedCheck);
            }
            pthread_mutex_lock(&check);
            end = false;
            for (int i = area.first, s1 = 0; (i < area.first + rowSpan) && !end; i++, s1++) {
                for (int j = area.second, s2 = 0; (j < area.second + colSpan) && !end; j++, s2++) {
                    if (i == lastFailedCoordinates.first && j == lastFailedCoordinates.second) {
                        lastFailedCheck = nullptr;
                        lastFailedCoordinates.first = -1;
                        lastFailedCoordinates.second = -1;
                        continue;
                    }
                    if (pthread_mutex_trylock(grid[i][j].second) != 0) {
                        lastFailedCheck = grid[i][j].second;
                        lastFailedCoordinates.first = i;
                        lastFailedCoordinates.second = j;
                        bool end2 = false;
                        for (int k = area.first; k < area.first + rowSpan && !end2; k++) {
                            for (int m = area.second; m < area.second + colSpan && !end2; m++) {
                                if (k == i && m == j) {end2 = true; break;}
                                pthread_mutex_unlock(grid[k][m].second);
                            }
                        }
                        end = true;
                    }
                }
            }
            pthread_mutex_unlock(&check);
        } while (lastFailedCheck);
        hw2_notify(PROPER_PRIVATE_ARRIVED, gid, area.first, area.second);
    }

    static void* mainRoutine(void* arg) {
        ProperPrivate* thisPrivate = (ProperPrivate*) arg;
        hw2_notify(PROPER_PRIVATE_CREATED, thisPrivate->gid, 0, 0);
        for (int i = 0; i < thisPrivate->areas.size(); i++) {
            thisPrivate->checkForTheArea(*(thisPrivate->areas[i]));
            thisPrivate->pickUpCigbutts(*(thisPrivate->areas[i]));
        }
        hw2_notify(PROPER_PRIVATE_EXITED, thisPrivate->gid, 0, 0);
    }

    void printInfo() {
        cout << "----" << endl;
        cout << "gid = " << gid << endl;
        cout << "rowSpan = " << rowSpan << endl;
        cout << "colSpan = " << colSpan << endl;
        cout << "numAreas = " << numAreas << endl;
        for (int i = 0; i < numAreas; i++) {
            cout << "area " << i + 1 << " = (" << areas[i]->first << ", " << areas[i]->second << ")" << endl;
        }
        cout << "----" << endl;
    }
};

vector<ProperPrivate*> privates;

int main() {
    int numRows, numCols, numPrivates;
    cin >> numRows >> numCols;
    grid.reserve(numRows);
    for (int i = 0; i < numRows; i++) {
        grid[i].reserve(numCols);
        for (int j = 0; j < numCols; j++) {
            cin >> grid[i][j].first;
            pthread_mutex_t* mutex = new pthread_mutex_t;
            pthread_mutex_init(mutex, nullptr);
            grid[i][j].second = mutex;
        }
    }
    cin >> numPrivates;
    for (int i = 0; i < numPrivates; i++) {
        int gid, rowSpan, colSpan, numAreas;
        long gatherTime;
        cin >> gid >> rowSpan >> colSpan >> gatherTime >> numAreas;
        ProperPrivate* newPrivate = new ProperPrivate(gid, rowSpan, colSpan, gatherTime, numAreas);
        for (int j = 0; j < numAreas; j++) {
            int row, col;
            cin >> row >> col;
            newPrivate->addArea(row, col);
        }
        privates.push_back(newPrivate);
    }
    hw2_init_notifier();
    pthread_t* threadIDs = new pthread_t[privates.size()];
    for (int i = 0; i < privates.size(); i++) {
        pthread_create(&threadIDs[i], nullptr, ProperPrivate::mainRoutine, privates[i]);
    }
    for (int i = 0; i < privates.size(); i++) {
        pthread_join(threadIDs[i], nullptr);
    }

    return 0;
}