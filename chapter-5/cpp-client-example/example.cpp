#include "clickhouse/columns/date.h"
#include "clickhouse/columns/string.h"
#include <cstddef>
#include <iostream>
#include <clickhouse/client.h>
#include <chrono>
#include <thread>

using namespace clickhouse;

void batch_insert_data(Client& client, size_t batch_rows, size_t batch_count, bool sleep)
{
    auto block = client.BeginInsert("INSERT INTO default.test_topic_parsed (event_time, event_type, event_data) SETTINGS insert_deduplicate = 0, max_execution_time=5 VALUES");
    for (size_t i = 0; i < batch_count; ++i)
    {
        auto event_time = block[0]->As<ColumnDateTime>();
        auto event_type = block[1]->As<ColumnString>();
        auto event_data = block[2]->As<ColumnString>();
        for (size_t j = 0; j < batch_rows; ++j)
        {
            event_time->Append(time(NULL));
            event_type->Append("click");
            event_data->Append(std::to_string(random()));
        }
        if (sleep)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10000));
        }
        block.RefreshRowCount();
        client.SendInsertBlock(block);
        block.Clear();
    }
    client.EndInsert();
}

int main(int argc, char* argv[])
{
    /// Initialize client connection.
    std::string insert_type = argv[1];
    size_t batch_rows = std::stoi(argv[2]);
    size_t batch_count = std::stoi(argv[3]);
    Client client(ClientOptions().SetHost("localhost").SetPort(9000));
    if (insert_type == "batch")
    {
        batch_insert_data(client, batch_rows, batch_count, false);
    }
    else if (insert_type == "sleep")
    {
        batch_insert_data(client, batch_rows, batch_count, true);
    }
    else
    {
        std::cerr << "Invalid insert type: " << insert_type << std::endl;
    }
    return 0;
}