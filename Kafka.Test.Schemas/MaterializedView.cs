using System.Collections.Concurrent;
using Newtonsoft.Json;

namespace Schemas;

public class MaterializedView
{
    private readonly ConcurrentDictionary<string, Stock> _dataStore;

    public MaterializedView()
    {
        _dataStore = new ConcurrentDictionary<string, Stock>();
    }

    public void Set(string key, Stock stock)
    {
        if (_dataStore.TryGetValue(key, out var value))
        {
            if (stock.UnixDate > value.UnixDate)
                _dataStore.TryUpdate(key, stock, value);
        }
        else
        {
            if (!_dataStore.TryAdd(key, stock))
            {
                Set(key, stock);
            }
        }
    }

    public Stock? Get(string key)
    {
        var exists = _dataStore.TryGetValue(key, out var value);
        return !exists ? null : value;
    }

    public string Loggify()
    {
        return JsonConvert.SerializeObject(InnerDict(),
            Formatting.Indented);
    }

    public Dictionary<string, Stock> InnerDict()
    {
        return _dataStore.ToDictionary(pair => pair.Key, pair => pair.Value);
    }
}

public record Stock(long UnixDate, decimal Price);