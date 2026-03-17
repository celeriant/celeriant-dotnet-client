FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ../Celeriant.Client/ Celeriant.Client/
COPY Celeriant.Demo/ Celeriant.Demo/
WORKDIR /src/Celeriant.Demo
RUN dotnet publish -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .
EXPOSE 8080
ENTRYPOINT ["dotnet", "Celeriant.Demo.dll"]
