#include <iostream>
#include <vector>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#include "hw2_output.h"
#include <errno.h>
using namespace std;

enum privateState {CREATED, AT_WORK, AT_BREAK, STOPPED};
enum order {BREAK, CONTINUE, STOP, FAIL};
pthread_mutex_t check = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t breakMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lastSignalledMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t continueSignalled = PTHREAD_COND_INITIALIZER;
pthread_mutex_t checkMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t checkCond = PTHREAD_COND_INITIALIZER;
vector< vector< pair<int, pthread_mutex_t*> > > grid;
vector<pair<double, enum order>*> orders;
vector<pair<double, enum order>*> privateOrders;
vector< vector<pthread_mutex_t*> > takenOrder;
static struct timeval start_time;
static struct timeval curr_time;
int lastSignalled = -1;
int numPrivates;
// get_timestamp -> ms * 1000 (us)

static long get_timestamp(void)
{
    struct timeval cur_time;
    gettimeofday(&cur_time, NULL);
    return (cur_time.tv_sec - start_time.tv_sec) * 1000000 
           + (cur_time.tv_usec - start_time.tv_usec);
}

void checkIfReceivedLastOrder(int index) {
    for (int i = 0; i < numPrivates; i++) {
        pthread_mutex_lock(takenOrder[i][index]);
        pthread_mutex_unlock(takenOrder[i][index]);
    }
}

class ProperPrivate {
public:
    int gid;
    int rowSpan;
    int colSpan;
    unsigned long gatherTime;
    int numAreas;
    enum privateState state;
    vector<pair<int, int>*> areas;
    vector<pthread_mutex_t*> orderResponseMutexes;
    pthread_mutex_t breakMutex;
    int currentAreaIndex;
    int currentOrderIndex;
    int privateIndex;

    ProperPrivate(int gid, int rowSpan, int colSpan, long gatherTime, int numAreas, int privateIndex)
    :   gid(gid),
        rowSpan(rowSpan),
        colSpan(colSpan),
        numAreas(numAreas),
        state(CREATED),
        gatherTime(gatherTime * 1000),
        currentOrderIndex(-1),
        currentAreaIndex(-1),
        privateIndex(privateIndex)
    {}

    ~ProperPrivate() {
        for (int i = 0; i < areas.size(); i++) {
            delete areas[i];
        }
    }

    void addArea(int row, int col) {
        pair<int, int>* newStart = new pair<int, int>(row, col);
        areas.push_back(newStart);
    }

    enum order getNextOrder() {
        if (currentOrderIndex + 1 < privateOrders.size()) {
            return privateOrders[currentOrderIndex + 1]->second;
        }
        return FAIL;
    }

    long getTimeToNextOrder(void) {
        if (currentOrderIndex + 1 < privateOrders.size()) {
            return privateOrders[currentOrderIndex + 1]->first - get_timestamp();
        }
        return -1;
    }

    bool shouldSleep(void) {
        long remainingTime = getTimeToNextOrder();
        if (remainingTime != -1)
            return gatherTime < remainingTime;
        return true;
    }

    void notifyOrderIfNecessary() {
        if (currentOrderIndex + 1 < privateOrders.size() && currentOrderIndex + 1 > lastSignalled && getNextOrder() != CONTINUE) {
            checkIfReceivedLastOrder(currentOrderIndex);
            lastSignalled = currentOrderIndex + 1;
            enum order nextOrder = getNextOrder();
            if (nextOrder == BREAK) {
                hw2_notify(ORDER_BREAK, 0, 0, 0);
            } else if (nextOrder == STOP) {
                hw2_notify(ORDER_STOP, 0, 0, 0);
            }
        }
    }

    bool shouldTakeOrder(void) {
        long remainingTime = getTimeToNextOrder();
        return remainingTime <= 0 && remainingTime != -1;
    }

    void synchronousLock(pthread_mutex_t* mutex) {
        // long timeToNextOrder = getTimeToNextOrder();
        // enum order nextOrder = getNextOrder();
        bool success = false;
        struct timespec remainingTime;
        while (!success) {
            if (getTimeToNextOrder() != -1 && getNextOrder() != CONTINUE) { // timedLock
                // cout << "time to next order = " << timeToNextOrder << endl;
                clock_gettime(CLOCK_REALTIME, &remainingTime);
                long nt = (remainingTime.tv_nsec + getTimeToNextOrder() * 1000);
                long st = nt / 1000000000;
                nt = nt % 1000000000;
                remainingTime.tv_nsec = nt;
                remainingTime.tv_sec += st;
                int a = pthread_mutex_timedlock(mutex, &remainingTime);
                //cout << "timedlock = " << a << " gid = " << gid << endl;
                if (a != 0 && get_timestamp() / 1000 >= privateOrders[currentOrderIndex + 1]->first / 1000) { // if it exitted because of an order
                    handleIncomingOrder(getNextOrder(), false);
                    if (currentOrderIndex < privateOrders.size()) {
                        pthread_mutex_unlock(takenOrder[privateIndex][currentOrderIndex]);
                    }
                    if (currentOrderIndex + 1 < privateOrders.size()) {
                        pthread_mutex_lock(takenOrder[privateIndex][currentOrderIndex + 1]);
                    }
                    continue;
                }
                break;
            } else {
                pthread_mutex_lock(mutex);
                break;
            }
        }
        
    }
    void synchronousWait(pthread_cond_t* cond, pthread_mutex_t* mutex) {
        int ret;
        if (getTimeToNextOrder() != -1 && getNextOrder() != CONTINUE) {
            struct timespec remainingTime;
            clock_gettime(CLOCK_REALTIME, &remainingTime);
            remainingTime.tv_nsec += (getTimeToNextOrder() * 1000);
            ret = pthread_cond_timedwait(cond, mutex, &remainingTime);
            if (ret != 0) { // exitted because of interrupt
                enum order nextOrder = getNextOrder();
                handleIncomingOrder(nextOrder, false);
                if (currentOrderIndex < privateOrders.size()) {
                    pthread_mutex_unlock(takenOrder[privateIndex][currentOrderIndex]);
                }
                if (currentOrderIndex + 1 < privateOrders.size()) {
                    pthread_mutex_lock(takenOrder[privateIndex][currentOrderIndex + 1]);
                }
                synchronousWait(cond, mutex);
            }
        } else {
            pthread_cond_wait(cond, mutex);
        }
    }

    void handleIncomingOrder(enum order incomingOrder, bool willSleep) {
        if (willSleep) {
            usleep(getTimeToNextOrder());
        }
        pthread_mutex_lock(&lastSignalledMutex);
        notifyOrderIfNecessary();
        pthread_mutex_unlock(&lastSignalledMutex);
        if (incomingOrder == BREAK) {
            hw2_notify(PROPER_PRIVATE_TOOK_BREAK, gid, 0, 0);
            currentOrderIndex++;
            pthread_mutex_lock(&breakMutex);
            pthread_cond_wait(&continueSignalled, &breakMutex);
            hw2_notify(PROPER_PRIVATE_CONTINUED, gid, 0, 0);
            currentOrderIndex++;
            pthread_mutex_unlock(&breakMutex);
        } else if (incomingOrder == STOP) {
            pthread_mutex_t stopMutex = PTHREAD_MUTEX_INITIALIZER;
            pthread_mutex_lock(&stopMutex);
            hw2_notify(PROPER_PRIVATE_STOPPED, gid, 0, 0);
            currentOrderIndex++;
            pthread_mutex_lock(&stopMutex);
        } else {
            currentOrderIndex++;
        }
    }

    void pickUpCigbutts(pair<int, int>& area) {
        long extraSleepTime = 0;
        for (int i = area.first; i < area.first + rowSpan; i++) {
            for (int j = area.second; j < area.second + colSpan; j++) {
                while (grid[i][j].first) {
                    enum order incomingOrder = getNextOrder();
                    if (!shouldSleep() && incomingOrder != CONTINUE && incomingOrder != FAIL) {
                        fallback(*(areas[currentAreaIndex]), i, j, true);
                        handleIncomingOrder(incomingOrder, true);
                        hw2_notify(PROPER_PRIVATE_ARRIVED, gid, i, j);
                        if (currentOrderIndex < privateOrders.size()) {
                            pthread_mutex_unlock(takenOrder[privateIndex][currentOrderIndex]);
                        }
                        if (currentOrderIndex + 1 < privateOrders.size()) {
                            pthread_mutex_lock(takenOrder[privateIndex][currentOrderIndex + 1]);
                        }
                        continue;
                    } else if (shouldTakeOrder() && incomingOrder != CONTINUE && incomingOrder != FAIL) {
                        fallback(*(areas[currentAreaIndex]), i, j, true);
                        handleIncomingOrder(incomingOrder, false);
                        hw2_notify(PROPER_PRIVATE_ARRIVED, gid, i, j);
                        if (currentOrderIndex < privateOrders.size()) {
                            pthread_mutex_unlock(takenOrder[privateIndex][currentOrderIndex]);
                        }
                        if (currentOrderIndex + 1 < privateOrders.size()) {
                            pthread_mutex_lock(takenOrder[privateIndex][currentOrderIndex + 1]);
                        }
                        continue;
                    } else {
                        usleep(gatherTime);
                    }
                    grid[i][j].first--;
                    hw2_notify(PROPER_PRIVATE_GATHERED, gid, i, j);
                }
                pthread_mutex_unlock(grid[i][j].second);
            }
        }
        hw2_notify(PROPER_PRIVATE_CLEARED, gid, 0, 0);
    }

    void fallback(pair<int, int>& area, int failedRow, int failedCol, bool lockedAndFailed) {
        bool completed = false;
        for (int k = area.first; k < area.first + rowSpan && !completed; k++) {
            for (int m = area.second; m < area.second + colSpan && !completed; m++) {
                if (k == failedRow && m == failedCol) {
                    if (lockedAndFailed) {
                        pthread_mutex_unlock(grid[k][m].second);
                    }
                    completed = true; 
                    break;
                }
                pthread_mutex_unlock(grid[k][m].second);
            }
        }
    }

    void checkForTheArea(pair<int, int>& area) {
        bool end = false;
        pair<int, int> lastFailedCoordinates(-1, -1);
        pthread_mutex_t* lastFailedCheck = nullptr;
        do {
            struct timespec remainingTime;
            long timeToNextOrder;
            enum order nextOrder;
            if (lastFailedCoordinates.first != -1 && lastFailedCoordinates.second != -1) {
                synchronousLock(lastFailedCheck);
                //cout << "locked " << lastFailedCoordinates.first << " " << lastFailedCoordinates.second << endl;
                pthread_mutex_unlock(lastFailedCheck);
                lastFailedCheck = nullptr;
                lastFailedCoordinates.first = -1;
                lastFailedCoordinates.second = -1;
            }
            synchronousLock(&check);
            end = false;
            for (int i = area.first; (i < area.first + rowSpan) && !end; i++) {
                for (int j = area.second; (j < area.second + colSpan) && !end; j++) {
                    if (pthread_mutex_trylock(grid[i][j].second) != 0) {
                        lastFailedCheck = grid[i][j].second;
                        lastFailedCoordinates.first = i;
                        lastFailedCoordinates.second = j;
                        fallback(area, i, j, false);
                        end = true;
                    }
                }
            }
            pthread_mutex_unlock(&check);
        } while (lastFailedCheck);
        currentAreaIndex++;
        hw2_notify(PROPER_PRIVATE_ARRIVED, gid, area.first, area.second);
    }

    static void* mainRoutine(void* arg) {
        ProperPrivate* thisPrivate = (ProperPrivate*) arg;
        hw2_notify(PROPER_PRIVATE_CREATED, thisPrivate->gid, 0, 0);
        for (int i = 0; i < thisPrivate->areas.size(); i++) {
            thisPrivate->state = AT_WORK;
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
    int numRows, numCols, numPrivates, numOrders;
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
        ProperPrivate* newPrivate = new ProperPrivate(gid, rowSpan, colSpan, gatherTime, numAreas, i);
        for (int j = 0; j < numAreas; j++) {
            int row, col;
            cin >> row >> col;
            newPrivate->addArea(row, col);
        }
        privates.push_back(newPrivate);
    }
    numPrivates = privates.size();
    cin >> numOrders;
    for (int i = 0; i < numOrders; i++) {
        double time;
        string order;
        pair <double, enum order>* p = new pair<double, enum order>;
        cin >> time >> order;
        p->first = time * 1000;
        if (order == "break") {
            p->second = BREAK;
        } else if (order == "continue") {
            p->second = CONTINUE;
        } else {
            p->second = STOP;
        }
        orders.push_back(p);
        if (p->second != CONTINUE || (i > 0 && orders[i - 1]->second == BREAK)) {
            privateOrders.push_back(p);
        }
    }
    cout << "numOrders = " << orders.size() << endl;
    cout << "numprivateorders = " << privateOrders.size() << endl;
    for (int j = 0; j < privates.size(); j++) {
        int numOrders = privateOrders.size();
        vector<pthread_mutex_t*> responseVector;
        for (int j = 0; j < numOrders; j++) {
            pthread_mutex_t* orderResponseMutex = new pthread_mutex_t;
            responseVector.push_back(orderResponseMutex);
        }
        takenOrder.push_back(responseVector);
    }
    gettimeofday(&start_time, nullptr);
    hw2_init_notifier();
    pthread_t* threadIDs = new pthread_t[privates.size()];
    for (int i = 0; i < privates.size(); i++) {
        pthread_create(&threadIDs[i], nullptr, ProperPrivate::mainRoutine, privates[i]);
    }
    int mainSignalIndex = -1;
    for (int i = 0; i < orders.size(); i++) {
        long timestamp = get_timestamp();
        usleep(orders[i]->first - timestamp);
        pthread_mutex_lock(&lastSignalledMutex);
        if (lastSignalled > mainSignalIndex) {
            mainSignalIndex = lastSignalled;
            pthread_mutex_unlock(&lastSignalledMutex);
            continue;
        }
        checkIfReceivedLastOrder(lastSignalled);
        if (orders[i]->second == CONTINUE) {
            hw2_notify(ORDER_CONTINUE, 0, 0, 0);
            if (i > 0 && orders[i - 1]->second == BREAK) {
                mainSignalIndex++;
                lastSignalled++;
                pthread_cond_broadcast(&continueSignalled);
            }
        } else if (orders[i]->second == BREAK) {
            //cout << "order break from main" << endl;
            lastSignalled++;
            mainSignalIndex++;
            hw2_notify(ORDER_BREAK, 0, 0, 0);
        } else {
            lastSignalled++;
            mainSignalIndex++;
            hw2_notify(ORDER_STOP, 0, 0, 0);
        }
        pthread_mutex_unlock(&lastSignalledMutex);
    }
    for (int i = 0; i < privates.size(); i++) {
        pthread_join(threadIDs[i], nullptr);
    }

    return 0;
}