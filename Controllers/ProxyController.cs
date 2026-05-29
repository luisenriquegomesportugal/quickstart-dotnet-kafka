using Microsoft.AspNetCore.Mvc;

namespace dotnet_webapi.Controllers;

[ApiController]
[Route("[controller]")]
public class ProxyController : ControllerBase
{
    private readonly HttpClient _httpClient;

    public ProxyController(IHttpClientFactory httpClientFactory)
    {
        _httpClient = httpClientFactory.CreateClient();
    }

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        var response = await _httpClient.GetAsync(
            "https://jsonplaceholder.typicode.com/todos/1"
        );

        var content = await response.Content.ReadAsStringAsync();

        return StatusCode((int)response.StatusCode, new
        {
            status = (int)response.StatusCode,
            success = response.IsSuccessStatusCode,
            headers = response.Headers.ToDictionary(
                h => h.Key,
                h => string.Join(",", h.Value)
            ),
            data = content
        });
    }
}