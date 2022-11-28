using Blazorise;
using Blazorise.Bootstrap;
using Blazorise.Icons.FontAwesome;
using Kafka.Test.Overview;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Kafka.Test.Overview.Data;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();
builder.Services.AddBlazorise(o => o.Immediate = true)
    .AddBootstrapProviders()
    .AddFontAwesomeIcons();


builder.Services.AddSingleton<LatencyService>();
builder.Services.AddHostedService<LatencyWorker>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseStaticFiles();

app.UseRouting();

app.MapBlazorHub();
app.MapFallbackToPage("/_Host");

app.Run();