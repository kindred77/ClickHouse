#include <bitset>
#include <Common/Stopwatch.h>
#include <common/range.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <iostream>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <boost/dynamic_bitset.hpp>

#include <rocksdb/db.h>
#include <rocksdb/table.h>

static const size_t rows = 60000;

void testDynamicBitSet()
{
    typedef uint8_t Block;
    typedef boost::dynamic_bitset<Block> Bitset;
    Bitset bitset(rows);
    Stopwatch watch;
    for (const auto i : collections::range(0, rows))
    {
        bitset.set(i);
    }
    watch.stop();

    std::vector<Block> bytes;
    boost::to_block_range(bitset, std::back_inserter(bytes));

    std::cout << "------dynamic_bitset<uint8_t>-----" << watch.elapsedSeconds() << "----" << bytes.size() << std::endl;

    watch.restart();
    for (size_t i = 0; i < 100; ++i)
    {
        Bitset bs(rows);
        bs.init_from_block_range(bytes.begin(), bytes.end());
    }
    watch.stop();
    std::cout << "------dynamic_bitset<uint8_t>----deseriallize 100 times-----" << watch.elapsedSeconds() << std::endl;
}

void testVector()
{
    std::vector<bool> v(rows);
    Stopwatch watch;
    for (const auto i : collections::range(0, rows))
    {
        v[i] = true;
    }

    watch.stop();
    std::cout << "------vector<bool>-----" << watch.elapsedSeconds() << "----" << sizeof(v) << std::endl;

    // unsigned long long  length = bit_table_.size();
    // unsigned long long  totalSize = length * sizeof(unsigned char);
    // fout.write((char*)&totalSize, sizeof(unsigned long long));
    // fout.write((char*)&bit_table_[0], totalSize);


    // unsigned long long length, totalSize;
    // ifile.read((char*)&length, sizeof(unsigned long long));
    // ifile.read((char*)&totalSize, sizeof(unsigned long long));

    // watch.restart();
    // for (size_t i = 0; i < 100; ++i)
    // {
    //     std::vector<bool> v;
    //     v.resize();
    //     bs.init_from_block_range(bytes.begin(), bytes.end());
    // }
}

void testBitSet()
{
    std::bitset<rows> bt;
    Stopwatch watch;
    for (const auto i : collections::range(0, rows))
    {
        bt.set(i);
    }
    watch.stop();
    
    std::cout << "------bitset-----" << watch.elapsedSeconds() << "----" << std::endl;
}

void testRoaringBitmapWithSmallSet()
{
    DB::RoaringBitmapWithSmallSet<UInt32, 32> rbs;
    Stopwatch watch;
    for (const auto i : collections::range(0, rows))
    {
        rbs.add(i);
    }
    watch.stop();
    std::cout << "------RoaringBitmapWithSmallSet-----" << watch.elapsedSeconds() << std::endl;
    //rbs.write()
}

void testRoaringBitmap()
{
    roaring::Roaring rb;
    Stopwatch watch;
    for (const auto i : collections::range(0, rows))
    {
        rb.add(i);
    }
    watch.stop();
    rb.runOptimize();
    std::cout << "------Roaring-----" << watch.elapsedSeconds() << "-----" << rb.getSizeInBytes() << std::endl;
    
    std::vector<char> buf(rb.getSizeInBytes());
    const auto writen_size = rb.write(buf.data());

    watch.restart();
    for (size_t i = 0; i < 100; ++i)
    {
        roaring::Roaring rr;
        rr.read(buf.data());
    }
    watch.stop();
    std::cout << "------Roaring----deseriallize 100 times-----" << watch.elapsedSeconds() << "-----writen_size:" << writen_size << std::endl;
}

void testRocksDB()
{
    rocksdb::Options options;
    rocksdb::DB * db;
    options.create_if_missing = true;
    options.compression = rocksdb::CompressionType::kZSTD;
    rocksdb::Status status = rocksdb::DB::Open(options, "./db_test", &db);

    if (status != rocksdb::Status::OK())
        throw DB::Exception("Fail to open rocksdb: " + status.ToString(), 1);
    auto rocksdb_ptr = std::shared_ptr<rocksdb::DB>(db);

    //------------insert into data--------------------
    rocksdb::WriteBatch batch;
    const auto rows_test = 100;
    const auto bm_size = 1000;
    std::vector<String> keys;
    keys.reserve(rows_test);
    for (size_t i = 0; i < rows_test; ++i)
    {
        roaring::Roaring r;
        //r.addRange(1, bm_size + 1);
        r.add(9);
        r.add(7);
        r.add(5);
        r.add(3);
        r.add(1);
        
        r.add(19);
        r.add(17);
        r.add(15);
        r.add(13);
        r.add(11);
        
        r.runOptimize();
        std::vector<char> buf(r.getSizeInBytes());
        const auto writen_size = r.write(buf.data());
        std::cout << "put size:-----" << r.cardinality() << "----writen_size: " << writen_size << "----getSizeInBytes: " << r.getSizeInBytes() << std::endl;
        keys.emplace_back("part_" + std::to_string(i));
        //String key = "part_" + std::to_string(i);
        const auto & val = std::string_view(buf.data(), writen_size);
        //std::cout << key << "----" << std::endl;
        batch.Put(rocksdb_ptr->DefaultColumnFamily(), keys.back(), i == 0 ? std::string_view("test string!") : val);
    }

    status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
        throw DB::Exception("Can not new marks for partition to RocksDB, write error: " + status.ToString(), 1);
    
    //-----------------read data and update-------------
    std::vector<rocksdb::Slice> key_slices;//(rows_test);
    key_slices.reserve(rows_test);
    std::vector<rocksdb::PinnableSlice> values(rows_test);
    std::vector<rocksdb::Status> statuses(rows_test);
    std::cout << key_slices.size() << "----" << values.size() << "----" << statuses.size() << std::endl;
    for (const auto & key : keys)
    {
        key_slices.emplace_back(key);
        //std::cout << "add key-----" << key_slices[i].ToString() << std::endl;
    }

    //---------------read single-----------
    const auto idx = 1;
    std::cout << "read single ------key: " << key_slices[idx].ToString() << std::endl;
    if (idx == 0)
    {
        String single_val;
        status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key_slices[idx], &single_val);
        if (!status.ok())
            throw DB::Exception("Can not read single, error: " + status.ToString(), 1);
        std::cout << "---value in 0:-----" << single_val << std::endl;
    }
    else
    {
        String single_val;
        status = rocksdb_ptr->Get(rocksdb::ReadOptions(), key_slices[idx], &single_val);
        if (!status.ok())
            throw DB::Exception("Can not read single, error: " + status.ToString(), 1);
        const roaring::Roaring & r = roaring::Roaring::readSafe(single_val.data(), single_val.size());
        std::cout << "---cardinality:-----" << r.cardinality() << "----" << single_val.size() << "----getSizeInBytes: " << r.getSizeInBytes() << std::endl;
        //transform to UInt8 column
        auto flag_col = DB::DataTypeUInt8().createColumnConst(6, 1)->convertToFullColumnIfConst()->assumeMutable();
        std::vector<UInt32> v;
        //std::cout << "type name: ---" << typeid(*flag_col).name() << std::endl;
        const auto card_in_bitmap = r.cardinality();
        if (6 > card_in_bitmap)
            throw DB::Exception("Rows to be read is more than cardinality in bitmap, it is a bug. ", 1);
        v.resize(card_in_bitmap);
        r.toUint32Array(v.data());
        auto & flag_data = typeid_cast<DB::ColumnUInt8 *>(flag_col.get())->getData();
        for (auto i : v)
        {
            if (i < 6)
                flag_data[i] = 0;
            else
                break;
            std::cout << "offset of record is: " << i << " is deleted." << std::endl;
        }

        for (size_t i = 0; i < flag_col->size(); ++i)
        {
            std::cout << "field: " << flag_col->getInt(i) << std::endl;
        }
        
    }
    
    // rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), rocksdb_ptr->DefaultColumnFamily(), rows_test, key_slices.data(), values.data(), statuses.data());
    // std::cout << key_slices.size() << "----" << values.size() << "----" << statuses.size() << std::endl;
    // rocksdb::WriteBatch batch2;
    // for (size_t i = 0; i < rows_test; ++i)
    // {
    //     if (statuses[i].ok())
    //     {
    //         //0 is for string testing
    //         if (i != 0)
    //         {
    //             //roaring::Roaring r = roaring::Roaring::readSafe(values[i].data(), values[i].size());
    //             std::cout << i << "---values " << i << " :-----size: " << values[i].size() << std::endl;
    //             roaring::Roaring r = roaring::Roaring::readSafe(values[i].data(), values[i].size());
    //             std::cout << i << "---cardinality:-----" << r.cardinality() << "----" << values[i].size() << std::endl;
    //             r.remove(1);
    //             std::cout << i << "---after remove 1-----" << r.cardinality() << std::endl;
    //             r.runOptimize();
    //             std::vector<char> buf(r.getSizeInBytes());
    //             const auto writen_size = r.write(buf.data());
    //             batch2.Put(rocksdb_ptr->DefaultColumnFamily(), key_slices[i], std::string_view(buf.data(), writen_size));
    //         }
            
    //     }
    //     else
    //     {
    //         //key = part_name + "_" + mark_field.get<String>();
    //         throw DB::Exception("Fail to read bitmap in rocksdb, it is a bug,msg: " + statuses[i].ToString(), 1);
    //     }
    // }
    // status = rocksdb_ptr->Write(rocksdb::WriteOptions(), &batch2);
    // if (!status.ok())
    //     throw DB::Exception("Can not update marks bitmap in rocksDB, write error: " + status.ToString(), 1);
}

void testRange()
{
    for (auto i : collections::range(0, 1))
    {
        std::cout << i << std::endl;
    }
}

int main(int, char **)
{
    //testDynamicBitSet();
    //testVector();
    //testBitSet();
    //testRoaringBitmap();

    //testRocksDB();

    testRange();
    
}
