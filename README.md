# ChunkSave

A Fabric 1.20.1 mod that completely prevents Out-Of-Memory (OOM) crashes when pre-generating massive world generation mods like JJThunder or Big Globe with tools like Chunky. 

## The Problem
When you pre-generate a Minecraft world, the game often creates new chunks much faster than your drive (SSD/HDD) can save them. Vanilla Minecraft handles this by keeping all those unsaved chunks in your RAM. If your drive can't keep up with the generation speed, your RAM eventually fills up completely, causing the game or server to crash.

## The Solution
ChunkSave fixes this by acting as a traffic controller between world generation and your hard drive. 

Instead of letting RAM fill up infinitely, ChunkSave sets a strict limit on how many chunks can wait in memory. If that limit is reached, it temporarily pauses world generation until your drive catches up. 

## Configuration
When you run the mod for the first time, it will generate a config file at `config/ChunkSave.json`. The default settings are optimized for most users, but you can tweak them if needed:

## Installation & Building

**To Install:**
Drop the `.jar` file into your `mods` folder. 
*Requires Fabric Loader 0.15 or higher and the Fabric API.*

**To Build:**
Make sure you have JDK 17 installed. Run `gradle build` in the project directory. The compiled mod will be located in `build/libs/`.
