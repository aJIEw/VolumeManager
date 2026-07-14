package moe.chensi.volume.data

import androidx.datastore.core.DataStore
import androidx.datastore.preferences.core.Preferences
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.stringPreferencesKey
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

class AppPreferencesStore(private val dataStore: DataStore<Preferences>) {
    companion object {
        private val key = stringPreferencesKey("apps")

        private val json = Json { ignoreUnknownKeys = true }

        private val logger = Logger.getLogger(AppPreferencesStore::class.java.name)
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val loadStarted = AtomicBoolean(false)
    private val saveRequests = Channel<Unit>(Channel.CONFLATED)

    @Serializable
    private data class SerializedState(
        val values: MutableList<AppPreferences>,
        val indices: MutableMap<String, Int>,
        val systemSliderVisibility: MutableMap<String, Boolean> = mutableMapOf()
    )

    private val lock = Any()
    private var state = SerializedState(mutableListOf(), mutableMapOf())

    init {
        scope.launch {
            for (ignored in saveRequests) {
                try {
                    val serialized = synchronized(lock) {
                        json.encodeToString(state)
                    }
                    dataStore.edit { preferences ->
                        preferences[key] = serialized
                    }
                } catch (error: Exception) {
                    if (error is CancellationException) {
                        throw error
                    }
                    logger.log(Level.SEVERE, "Failed to save preferences", error)
                }
            }
        }
    }

    val values: List<AppPreferences>
        get() = state.values
    val indices: Map<String, Int>
        get() = synchronized(lock) { state.indices.toMap() }
    fun getSystemSliderVisible(id: String): Boolean {
        return synchronized(lock) { state.systemSliderVisibility[id] ?: true }
    }

    fun setSystemSliderVisible(id: String, value: Boolean) {
        val changed = synchronized(lock) {
            val oldValue = state.systemSliderVisibility[id] ?: true
            if (oldValue == value) {
                return@synchronized false
            }

            val updated = state.systemSliderVisibility.toMutableMap()
            updated[id] = value
            state = state.copy(systemSliderVisibility = updated)
            true
        }

        if (changed) {
            save()
        }
    }

    var systemSliderVisibility: Map<String, Boolean>
        get() = synchronized(lock) { state.systemSliderVisibility.toMap() }
        set(value) {
            val changed = synchronized(lock) {
                if (state.systemSliderVisibility == value) {
                    return@synchronized false
                }

                state = state.copy(systemSliderVisibility = value.toMutableMap())
                true
            }

            if (changed) {
                save()
            }
        }

    fun loadOnce(onLoaded: () -> Unit) {
        if (!loadStarted.compareAndSet(false, true)) {
            return
        }
        scope.launch {
            try {
                val preferences = dataStore.data.first()
                val valueJson = preferences[key]
                if (valueJson != null) {
                    synchronized(lock) {
                        state = json.decodeFromString<SerializedState>(valueJson)
                    }
                }
            } catch (error: Exception) {
                loadStarted.set(false)
                if (error is CancellationException) {
                    throw error
                }
                logger.log(Level.SEVERE, "Failed to load preferences", error)
                return@launch
            }

            onLoaded()
        }
    }

    fun getOrCreate(packageName: String): AppPreferences {
        synchronized(lock) {
            val index = state.indices[packageName]
            if (index != null) {
                return state.values[index]
            }

            val value = AppPreferences()
            state.indices[packageName] = state.values.size
            state.values.add(value)
            return value
        }
    }

    fun save() {
        saveRequests.trySend(Unit)
    }
}
